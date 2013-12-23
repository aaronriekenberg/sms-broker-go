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
  clientIDToClientState map[string]*client
}

func newTopic() *topic {
  return &topic{
    clientIDToClientState: make(map[string]*client),
  }
}

func (t *topic) addClient(c *client) {
  t.mutex.Lock()
  defer t.mutex.Unlock()

  t.clientIDToClientState[c.getUniqueID()] = c
}

func (t *topic) removeClient(c *client) {
  t.mutex.Lock()
  defer t.mutex.Unlock()

  delete(t.clientIDToClientState, c.getUniqueID())
}

func (t *topic) sendMessagePayload(payload []byte) {
  t.mutex.Lock()
  clients := make([]*client, len(t.clientIDToClientState))
  i := 0
  for _, client := range t.clientIDToClientState {
    clients[i] = client
    i += 1
  }
  t.mutex.Unlock()

  for _, client := range clients {
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

func (b *Broker) Run() {
  local, err := net.Listen(netString, b.listenAddress)
  if err != nil {
    logger.Fatalf("cannot listen: %v", err)
  }
  logger.Printf("listening on %v", b.listenAddress)
  for {
    clientConnection, err := local.Accept()
    if err != nil {
      logger.Printf("accept failed: %v", err)
    } else {
      c := newClient(clientConnection, b)
      c.start()
    }
  }
}

func (b *Broker) subscribeToTopic(topicName string, c *client) {
  b.mutex.Lock()
  topic, ok := b.topicNameToTopic[topicName]
  if !ok {
    topic = newTopic()
    logger.Printf("create topic %v", topicName)
    b.topicNameToTopic[topicName] = topic
  }
  b.mutex.Unlock()

  topic.addClient(c)
}

func (b *Broker) unsubscribeFromTopic(topicName string, c *client) {
  b.mutex.Lock()
  topic, ok := b.topicNameToTopic[topicName]
  b.mutex.Unlock()

  if ok {
    topic.removeClient(c)
  }
}

func (b *Broker) unsubscribeFromAllTopics(c *client) {
  b.mutex.Lock()
  topics := make([]*topic, len(b.topicNameToTopic))
  i := 0
  for _, topic := range b.topicNameToTopic {
    topics[i] = topic
    i += 1
  }
  b.mutex.Unlock()

  for _, topic := range topics {
    topic.removeClient(c)
  }
}

func (b *Broker) sendMessagePayloadToTopic(topicName string, messagePayload []byte) {
  b.mutex.Lock()
  topic, ok := b.topicNameToTopic[topicName]
  b.mutex.Unlock()

  if ok {
    topic.sendMessagePayload(messagePayload)
  }
}

type client struct {
  uniqueID         string
  connectionString string
  connection       net.Conn
  broker           *Broker
  writeChannel     chan []byte
  mutex            sync.Mutex
  destroyed        bool
}

func newClient(clientConnection net.Conn, broker *Broker) *client {
  return &client{
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

func (c *client) getUniqueID() string {
  return c.uniqueID
}

func (c *client) start() {
  logger.Printf("connect client %v %v", c.uniqueID, c.connectionString)
  go c.writeToClient()
  go c.readFromClient()
}

func (c *client) destroy() {
  c.mutex.Lock()
  if !c.destroyed {
    logger.Printf("disconnect client %v %v", c.uniqueID, c.connectionString)
    c.destroyed = true
    c.connection.Close()
    close(c.writeChannel)
  }
  c.mutex.Unlock()

  c.broker.unsubscribeFromAllTopics(c)
}

func (c *client) writeMessagePayload(payload []byte) {
  c.mutex.Lock()
  defer c.mutex.Unlock()

  if !c.destroyed {
    c.writeChannel <- payload
  }
}

func (c *client) writeToClient() {
  defer logger.Printf("writeToClient exit %v", c.uniqueID)

  connectionClosed := false
  headerBuffer := make([]byte, 4)
  for {
    payloadBuffer, ok := <-c.writeChannel
    if !ok {
      return
    }

    if !connectionClosed {
      binary.BigEndian.PutUint32(headerBuffer, uint32(len(payloadBuffer)))

      _, err := c.connection.Write(headerBuffer)
      if err != nil {
        logger.Printf("error writing header %v", err)
        c.connection.Close()
        connectionClosed = true
        continue
      }

      _, err = c.connection.Write(payloadBuffer)
      if err != nil {
        logger.Printf("error writing payload %v", err)
        c.connection.Close()
        connectionClosed = true
        continue
      }
    }
  }
}

func (c *client) readFromClient() {
  defer logger.Printf("readFromClient exit %v", c.uniqueID)
  defer c.destroy()

  headerBuffer := make([]byte, 4)
  for {
    _, err := io.ReadFull(c.connection, headerBuffer)
    if err != nil {
      logger.Printf("io.ReadFull header error %v", err)
      break
    }

    payloadSize := binary.BigEndian.Uint32(headerBuffer)
    if payloadSize == 0 {
      logger.Printf("payloadSize == 0")
      break
    }

    bodyBuffer := make([]byte, payloadSize)
    _, err = io.ReadFull(c.connection, bodyBuffer)
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

    err = c.processIncomingMessage(clientToBrokerMessage)
    if err != nil {
      break
    }
  }
}

func (c *client) processIncomingMessage(message *sms_protocol_protobuf.ClientToBrokerMessage) (err error) {
  switch message.GetMessageType() {
  case sms_protocol_protobuf.ClientToBrokerMessage_CLIENT_SUBSCRIBE_TO_TOPIC:
    c.broker.subscribeToTopic(message.GetTopicName(), c)

  case sms_protocol_protobuf.ClientToBrokerMessage_CLIENT_UNSUBSCRIBE_FROM_TOPIC:
    c.broker.unsubscribeFromTopic(message.GetTopicName(), c)

  case sms_protocol_protobuf.ClientToBrokerMessage_CLIENT_SEND_MESSAGE_TO_TOPIC:
    brokerToClientMessage := new(sms_protocol_protobuf.BrokerToClientMessage)
    brokerToClientMessage.MessageType =
      sms_protocol_protobuf.BrokerToClientMessage_BROKER_TOPIC_MESSAGE_PUBLISH.Enum()
    brokerToClientMessage.TopicName = proto.String(message.GetTopicName())
    brokerToClientMessage.MessagePayload = message.GetMessagePayload()

    var messagePayload []byte
    messagePayload, err = proto.Marshal(brokerToClientMessage)
    if err != nil {
      logger.Printf("proto.Marshal error %v", err)
    } else {
      c.broker.sendMessagePayloadToTopic(message.GetTopicName(), messagePayload)
    }
  }
  return
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
