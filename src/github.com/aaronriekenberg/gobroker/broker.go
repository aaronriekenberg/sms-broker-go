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
	topicWriteQueueSize  = 100
)

var (
	logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)
)

type Topic interface {
	AddClient(c Client)

	RemoveClient(c Client)

	PublishMessagePayload(payload []byte)
}

type Broker interface {
	Run()
}

type BrokerService interface {
	SubscribeToTopic(topicName string, c Client)

	UnsubscribeFromTopic(topicName string, c Client)

	UnsubscribeFromAllTopics(c Client)

	PublishMessagePayloadToTopic(topicName string, messagePayload []byte)
}

type Client interface {
	Start()

	UniqueID() string

	WriteMessagePayload(payload []byte)
}

type topicAction struct {
	addClient      Client
	removeClient   Client
	publishMessage []byte
}

type topic struct {
	writeChannel     chan *topicAction
	clientIDToClient map[string]Client
}

func NewTopic() Topic {
	t := &topic{
		writeChannel:     make(chan *topicAction, topicWriteQueueSize),
		clientIDToClient: make(map[string]Client),
	}
	go t.processActions()
	return t
}

func (t *topic) processActions() {
	for {
		action := <-t.writeChannel
		if action.publishMessage != nil {
			for _, client := range t.clientIDToClient {
				client.WriteMessagePayload(action.publishMessage)
			}
		} else if action.addClient != nil {
			t.clientIDToClient[action.addClient.UniqueID()] = action.addClient
		} else if action.removeClient != nil {
			delete(t.clientIDToClient, action.removeClient.UniqueID())
		}
	}
}

func (t *topic) AddClient(c Client) {
	t.writeChannel <- &topicAction{addClient: c}
}

func (t *topic) RemoveClient(c Client) {
	t.writeChannel <- &topicAction{removeClient: c}
}

func (t *topic) PublishMessagePayload(payload []byte) {
	t.writeChannel <- &topicAction{publishMessage: payload}
}

type broker struct {
	listenAddresses  []string
	mutex            sync.RWMutex
	topicNameToTopic map[string]Topic
}

func NewBroker(listenAddresses []string) Broker {
	return &broker{
		listenAddresses:  listenAddresses,
		topicNameToTopic: make(map[string]Topic),
	}
}

func (b *broker) Run() {
	for _, listenAddress := range b.listenAddresses[:len(b.listenAddresses)-1] {
		go b.listen(listenAddress)
	}
	b.listen(b.listenAddresses[len(b.listenAddresses)-1])
}

func (b *broker) listen(listenAddress string) {
	local, err := net.Listen(netString, listenAddress)
	if err != nil {
		logger.Fatalf("cannot listen: %v", err)
	}
	logger.Printf("listening on %v", listenAddress)
	for {
		clientConnection, err := local.Accept()
		if err != nil {
			logger.Printf("accept failed: %v", err)
		} else {
			c := NewClient(clientConnection, b)
			c.Start()
		}
	}
}

func (b *broker) SubscribeToTopic(topicName string, c Client) {
	topic := b.getTopic(topicName)

	if topic == nil {
		b.mutex.Lock()
		topic = b.topicNameToTopic[topicName]
		if topic == nil {
			topic = NewTopic()
			logger.Printf("create topic '%v'", topicName)
			b.topicNameToTopic[topicName] = topic
		}
		b.mutex.Unlock()
	}

	topic.AddClient(c)
}

func (b *broker) getTopic(topicName string) (t Topic) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	t = b.topicNameToTopic[topicName]
	return
}

func (b *broker) getAllTopics() (topics []Topic) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	topics = make([]Topic, len(b.topicNameToTopic))
	i := 0
	for _, topic := range b.topicNameToTopic {
		topics[i] = topic
		i += 1
	}
	return
}

func (b *broker) UnsubscribeFromTopic(topicName string, c Client) {
	topic := b.getTopic(topicName)
	if topic != nil {
		topic.RemoveClient(c)
	}
}

func (b *broker) UnsubscribeFromAllTopics(c Client) {
	topics := b.getAllTopics()
	for _, topic := range topics {
		topic.RemoveClient(c)
	}
}

func (b *broker) PublishMessagePayloadToTopic(topicName string, messagePayload []byte) {
	topic := b.getTopic(topicName)
	if topic != nil {
		topic.PublishMessagePayload(messagePayload)
	}
}

type client struct {
	uniqueID         string
	connectionString string
	connection       net.Conn
	brokerService    BrokerService
	writeChannel     chan []byte
	destroyedMutex   sync.RWMutex
	destroyed        bool
}

func NewClient(clientConnection net.Conn, brokerService BrokerService) Client {
	return &client{
		uniqueID:         uuid.New(),
		connectionString: buildClientConnectionString(clientConnection),
		connection:       clientConnection,
		brokerService:    brokerService,
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

func (c *client) UniqueID() string {
	return c.uniqueID
}

func (c *client) Start() {
	logger.Printf("connect client %v %v", c.uniqueID, c.connectionString)
	go c.writeToClient()
	go c.readFromClient()
}

func (c *client) destroy() {
	c.destroyedMutex.Lock()
	if !c.destroyed {
		logger.Printf("disconnect client %v %v", c.uniqueID, c.connectionString)
		c.destroyed = true
		c.connection.Close()
		close(c.writeChannel)
	}
	c.destroyedMutex.Unlock()

	c.brokerService.UnsubscribeFromAllTopics(c)
}

func (c *client) WriteMessagePayload(payload []byte) {
	c.destroyedMutex.RLock()
	defer c.destroyedMutex.RUnlock()

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
		c.brokerService.SubscribeToTopic(message.GetTopicName(), c)

	case sms_protocol_protobuf.ClientToBrokerMessage_CLIENT_UNSUBSCRIBE_FROM_TOPIC:
		c.brokerService.UnsubscribeFromTopic(message.GetTopicName(), c)

	case sms_protocol_protobuf.ClientToBrokerMessage_CLIENT_SEND_MESSAGE_TO_TOPIC:
		brokerToClientMessage := &sms_protocol_protobuf.BrokerToClientMessage{
			MessageType:    sms_protocol_protobuf.BrokerToClientMessage_BROKER_TOPIC_MESSAGE_PUBLISH.Enum(),
			TopicName:      proto.String(message.GetTopicName()),
			MessagePayload: message.GetMessagePayload(),
		}

		var messagePayload []byte
		messagePayload, err = proto.Marshal(brokerToClientMessage)
		if err != nil {
			logger.Printf("proto.Marshal error %v", err)
		} else {
			c.brokerService.PublishMessagePayloadToTopic(message.GetTopicName(), messagePayload)
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
	if len(os.Args) < 2 {
		logger.Fatalf("usage: %v <listen address> [<listen address> ...]", os.Args[0])
	}

	setNumProcs()

	broker := NewBroker(os.Args[1:])
	broker.Run()
}
