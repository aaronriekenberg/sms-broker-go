package main

import (
  "code.google.com/p/go-uuid/uuid"
  "code.google.com/p/goprotobuf/proto"
  "encoding/binary"
  "fmt"
  "github.com/aaronriekenberg/sms_protocol_protobuf"
  "io"
  "log"
  "net"
  "os"
  "runtime"
  "sync"
)

const (
  netString            = "tcp"
  clientWriteQueueSize = 100
)

var (
  logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)
)

type topic struct {
  mutex                 sync.Mutex
  clientIDToClientState map[string]*clientState
}

func newTopic() *topic {
  return &topic{
    clientIDToClientState: make(map[string]*clientState),
  }
}

func (t *topic) addClient(client *clientState) {
  t.mutex.Lock()
  defer t.mutex.Unlock()

  t.clientIDToClientState[client.getUniqueID()] = client
}

func (t *topic) removeClient(client *clientState) {
  t.mutex.Lock()
  defer t.mutex.Unlock()

  delete(t.clientIDToClientState, client.getUniqueID())
}

func (t *topic) sendMessagePayload(payload []byte) {
  t.mutex.Lock()
  defer t.mutex.Unlock()

  for _, client := range t.clientIDToClientState {
    client.writeMessagePayload(payload)
  }
}

type Broker struct {
  listenAddress    string
  mutex            sync.Mutex
  topicNameToTopic map[string]*topic
}

func NewBroker(listenAddress string) *Broker {
  return &Broker{
    listenAddress:    listenAddress,
    topicNameToTopic: make(map[string]*topic),
  }
}

func (broker *Broker) Run() {
  local, err := net.Listen(netString, broker.listenAddress)
  if err != nil {
    logger.Fatal("cannot listen: ", err)
  }
  logger.Printf("listening on %v", broker.listenAddress)
  for {
    clientConnection, err := local.Accept()
    if err != nil {
      logger.Printf("accept failed: %v", err)
    } else {
      client := newClientState(clientConnection, broker)
      client.start()
    }
  }
}

func (broker *Broker) subscribeToTopic(topicName string, client *clientState) {
  broker.mutex.Lock()
  topic, ok := broker.topicNameToTopic[topicName]
  if !ok {
    topic = newTopic()
    logger.Printf("create topic %v", topicName)
    broker.topicNameToTopic[topicName] = topic
  }
  broker.mutex.Unlock()

  topic.addClient(client)
}

func (broker *Broker) unsubscribeFromTopic(topicName string, client *clientState) {
  broker.mutex.Lock()
  topic, ok := broker.topicNameToTopic[topicName]
  broker.mutex.Unlock()

  if ok {
    topic.removeClient(client)
  }
}

func (broker *Broker) sendMessagePayloadToTopic(topicName string, messagePayload []byte) {
  broker.mutex.Lock()
  topic, ok := broker.topicNameToTopic[topicName]
  broker.mutex.Unlock()

  if ok {
    topic.sendMessagePayload(messagePayload)
  }
}

func (broker *Broker) destroyClient(client *clientState) {
  broker.mutex.Lock()
  for _, topic := range broker.topicNameToTopic {
    topic.removeClient(client)
  }
  broker.mutex.Unlock()

  client.destroy()
}

type clientState struct {
  uniqueID         string
  connectionString string
  connection       net.Conn
  broker           *Broker
  writeChannel     chan []byte
  mutex            sync.Mutex
  destroyed        bool
}

func newClientState(clientConnection net.Conn, broker *Broker) *clientState {
  return &clientState{
    uniqueID:         uuid.New(),
    connectionString: buildClientConnectionString(clientConnection),
    connection:       clientConnection,
    broker:           broker,
    writeChannel:     make(chan []byte, clientWriteQueueSize),
    destroyed:        false,
  }
}

func buildClientConnectionString(clientConnection net.Conn) string {
  return fmt.Sprintf(
    "%v -> %v",
    clientConnection.RemoteAddr(),
    clientConnection.LocalAddr())
}

func (client *clientState) getUniqueID() string {
  return client.uniqueID
}

func (client *clientState) start() {
  logger.Printf("connect client %v %v", client.uniqueID, client.connectionString)
  go client.writeToClient()
  go client.readFromClient()
}

func (client *clientState) destroy() {
  client.mutex.Lock()
  defer client.mutex.Unlock()

  if !client.destroyed {
    logger.Printf("disconnect client %v %v", client.uniqueID, client.connectionString)
    client.destroyed = true
    client.connection.Close()
    close(client.writeChannel)
  }
}

func (client *clientState) writeMessagePayload(payload []byte) {
  client.mutex.Lock()
  defer client.mutex.Unlock()

  if !client.destroyed {
    client.writeChannel <- payload
  }
}

func (client *clientState) writeToClient() {
  defer logger.Printf("writeToClient exit %v", client.uniqueID)

  connectionClosed := false
  headerBuffer := make([]byte, 4)
  for {
    payloadBuffer, ok := <-client.writeChannel
    if !ok {
      return
    }

    if !connectionClosed {
      binary.BigEndian.PutUint32(headerBuffer, uint32(len(payloadBuffer)))

      _, err := client.connection.Write(headerBuffer)
      if err != nil {
        logger.Printf("error writing header %v", err)
        client.connection.Close()
        connectionClosed = true
        continue
      }

      _, err = client.connection.Write(payloadBuffer)
      if err != nil {
        logger.Printf("error writing payload %v", err)
        client.connection.Close()
        connectionClosed = true
        continue
      }
    }
  }
}

func (client *clientState) readFromClient() {
  defer logger.Printf("readFromClient exit %v", client.uniqueID)
  defer client.broker.destroyClient(client)

  headerBuffer := make([]byte, 4)
  for {
    _, err := io.ReadFull(client.connection, headerBuffer)
    if err != nil {
      logger.Printf("io.ReadFull header error %v", err)
      break
    }

    payloadSize := binary.BigEndian.Uint32(headerBuffer)
    //logger.Printf("payloadSize %v", payloadSize)
    if payloadSize == 0 {
      logger.Printf("payloadSize == 0")
      break
    }

    bodyBuffer := make([]byte, payloadSize)
    _, err = io.ReadFull(client.connection, bodyBuffer)
    if err != nil {
      logger.Printf("io.ReadFull body error %v", err)
      break
    }

    clientToBrokerMessage := new(sms_protocol_protobuf.ClientToBrokerMessage)
    err = proto.Unmarshal(bodyBuffer, clientToBrokerMessage)
    if err != nil {
      logger.Printf("proto.Unmarshal error %v", err)
      break
    }

    client.processIncomingMessage(clientToBrokerMessage)
  }
}

func (client *clientState) processIncomingMessage(message *sms_protocol_protobuf.ClientToBrokerMessage) {
  //logger.Printf("processIncomingMessage %v", message)

  switch message.GetMessageType() {
  case sms_protocol_protobuf.ClientToBrokerMessage_CLIENT_SUBSCRIBE_TO_TOPIC:
    client.broker.subscribeToTopic(message.GetTopicName(), client)

  case sms_protocol_protobuf.ClientToBrokerMessage_CLIENT_UNSUBSCRIBE_FROM_TOPIC:
    client.broker.unsubscribeFromTopic(message.GetTopicName(), client)

  case sms_protocol_protobuf.ClientToBrokerMessage_CLIENT_SEND_MESSAGE_TO_TOPIC:
    brokerToClientMessage := new(sms_protocol_protobuf.BrokerToClientMessage)
    brokerToClientMessage.MessageType = sms_protocol_protobuf.BrokerToClientMessage_BROKER_TOPIC_MESSAGE_PUBLISH.Enum()
    brokerToClientMessage.TopicName = proto.String(message.GetTopicName())
    brokerToClientMessage.MessagePayload = message.GetMessagePayload()
    messagePayload, err := proto.Marshal(brokerToClientMessage)
    if err != nil {
      logger.Printf("proto.Marshal error %v", err)
    }
    client.broker.sendMessagePayloadToTopic(message.GetTopicName(), messagePayload)
  }
}

func setNumProcs() {
  newMaxProcs := runtime.NumCPU()
  prevMaxProcs := runtime.GOMAXPROCS(newMaxProcs)
  logger.Printf(
    "set GOMAXPROCS = %v, prev GOMAXPROCS = %v",
    newMaxProcs, prevMaxProcs)
}

func main() {
  if len(os.Args) != 2 {
    logger.Fatalf("usage: %v <listen address>", os.Args[0])
  }

  setNumProcs()

  broker := NewBroker(os.Args[1])
  broker.Run()
}
