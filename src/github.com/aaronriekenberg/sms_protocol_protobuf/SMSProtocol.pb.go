// Code generated by protoc-gen-go.
// source: SMSProtocol.proto
// DO NOT EDIT!

/*
Package sms_protocol_protobuf is a generated protocol buffer package.

It is generated from these files:
	SMSProtocol.proto

It has these top-level messages:
	ClientToBrokerMessage
	BrokerToClientMessage
*/
package sms_protocol_protobuf

import proto "github.com/golang/protobuf/proto"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

type ClientToBrokerMessage_ClientToBrokerMessageType int32

const (
	ClientToBrokerMessage_CLIENT_SUBSCRIBE_TO_TOPIC     ClientToBrokerMessage_ClientToBrokerMessageType = 0
	ClientToBrokerMessage_CLIENT_UNSUBSCRIBE_FROM_TOPIC ClientToBrokerMessage_ClientToBrokerMessageType = 1
	ClientToBrokerMessage_CLIENT_SEND_MESSAGE_TO_TOPIC  ClientToBrokerMessage_ClientToBrokerMessageType = 2
)

var ClientToBrokerMessage_ClientToBrokerMessageType_name = map[int32]string{
	0: "CLIENT_SUBSCRIBE_TO_TOPIC",
	1: "CLIENT_UNSUBSCRIBE_FROM_TOPIC",
	2: "CLIENT_SEND_MESSAGE_TO_TOPIC",
}
var ClientToBrokerMessage_ClientToBrokerMessageType_value = map[string]int32{
	"CLIENT_SUBSCRIBE_TO_TOPIC":     0,
	"CLIENT_UNSUBSCRIBE_FROM_TOPIC": 1,
	"CLIENT_SEND_MESSAGE_TO_TOPIC":  2,
}

func (x ClientToBrokerMessage_ClientToBrokerMessageType) Enum() *ClientToBrokerMessage_ClientToBrokerMessageType {
	p := new(ClientToBrokerMessage_ClientToBrokerMessageType)
	*p = x
	return p
}
func (x ClientToBrokerMessage_ClientToBrokerMessageType) String() string {
	return proto.EnumName(ClientToBrokerMessage_ClientToBrokerMessageType_name, int32(x))
}
func (x *ClientToBrokerMessage_ClientToBrokerMessageType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(ClientToBrokerMessage_ClientToBrokerMessageType_value, data, "ClientToBrokerMessage_ClientToBrokerMessageType")
	if err != nil {
		return err
	}
	*x = ClientToBrokerMessage_ClientToBrokerMessageType(value)
	return nil
}

type BrokerToClientMessage_BrokerToClientMessageType int32

const (
	BrokerToClientMessage_BROKER_TOPIC_MESSAGE_PUBLISH BrokerToClientMessage_BrokerToClientMessageType = 0
)

var BrokerToClientMessage_BrokerToClientMessageType_name = map[int32]string{
	0: "BROKER_TOPIC_MESSAGE_PUBLISH",
}
var BrokerToClientMessage_BrokerToClientMessageType_value = map[string]int32{
	"BROKER_TOPIC_MESSAGE_PUBLISH": 0,
}

func (x BrokerToClientMessage_BrokerToClientMessageType) Enum() *BrokerToClientMessage_BrokerToClientMessageType {
	p := new(BrokerToClientMessage_BrokerToClientMessageType)
	*p = x
	return p
}
func (x BrokerToClientMessage_BrokerToClientMessageType) String() string {
	return proto.EnumName(BrokerToClientMessage_BrokerToClientMessageType_name, int32(x))
}
func (x *BrokerToClientMessage_BrokerToClientMessageType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(BrokerToClientMessage_BrokerToClientMessageType_value, data, "BrokerToClientMessage_BrokerToClientMessageType")
	if err != nil {
		return err
	}
	*x = BrokerToClientMessage_BrokerToClientMessageType(value)
	return nil
}

type ClientToBrokerMessage struct {
	MessageType      *ClientToBrokerMessage_ClientToBrokerMessageType `protobuf:"varint,1,req,name=messageType,enum=sms.protocol.protobuf.ClientToBrokerMessage_ClientToBrokerMessageType" json:"messageType,omitempty"`
	TopicName        *string                                          `protobuf:"bytes,2,req,name=topicName" json:"topicName,omitempty"`
	MessagePayload   []byte                                           `protobuf:"bytes,3,opt,name=messagePayload" json:"messagePayload,omitempty"`
	XXX_unrecognized []byte                                           `json:"-"`
}

func (m *ClientToBrokerMessage) Reset()         { *m = ClientToBrokerMessage{} }
func (m *ClientToBrokerMessage) String() string { return proto.CompactTextString(m) }
func (*ClientToBrokerMessage) ProtoMessage()    {}

func (m *ClientToBrokerMessage) GetMessageType() ClientToBrokerMessage_ClientToBrokerMessageType {
	if m != nil && m.MessageType != nil {
		return *m.MessageType
	}
	return ClientToBrokerMessage_CLIENT_SUBSCRIBE_TO_TOPIC
}

func (m *ClientToBrokerMessage) GetTopicName() string {
	if m != nil && m.TopicName != nil {
		return *m.TopicName
	}
	return ""
}

func (m *ClientToBrokerMessage) GetMessagePayload() []byte {
	if m != nil {
		return m.MessagePayload
	}
	return nil
}

type BrokerToClientMessage struct {
	MessageType      *BrokerToClientMessage_BrokerToClientMessageType `protobuf:"varint,1,req,name=messageType,enum=sms.protocol.protobuf.BrokerToClientMessage_BrokerToClientMessageType" json:"messageType,omitempty"`
	TopicName        *string                                          `protobuf:"bytes,2,req,name=topicName" json:"topicName,omitempty"`
	MessagePayload   []byte                                           `protobuf:"bytes,3,opt,name=messagePayload" json:"messagePayload,omitempty"`
	XXX_unrecognized []byte                                           `json:"-"`
}

func (m *BrokerToClientMessage) Reset()         { *m = BrokerToClientMessage{} }
func (m *BrokerToClientMessage) String() string { return proto.CompactTextString(m) }
func (*BrokerToClientMessage) ProtoMessage()    {}

func (m *BrokerToClientMessage) GetMessageType() BrokerToClientMessage_BrokerToClientMessageType {
	if m != nil && m.MessageType != nil {
		return *m.MessageType
	}
	return BrokerToClientMessage_BROKER_TOPIC_MESSAGE_PUBLISH
}

func (m *BrokerToClientMessage) GetTopicName() string {
	if m != nil && m.TopicName != nil {
		return *m.TopicName
	}
	return ""
}

func (m *BrokerToClientMessage) GetMessagePayload() []byte {
	if m != nil {
		return m.MessagePayload
	}
	return nil
}

func init() {
	proto.RegisterEnum("sms.protocol.protobuf.ClientToBrokerMessage_ClientToBrokerMessageType", ClientToBrokerMessage_ClientToBrokerMessageType_name, ClientToBrokerMessage_ClientToBrokerMessageType_value)
	proto.RegisterEnum("sms.protocol.protobuf.BrokerToClientMessage_BrokerToClientMessageType", BrokerToClientMessage_BrokerToClientMessageType_name, BrokerToClientMessage_BrokerToClientMessageType_value)
}
