package broker

import (
	"strconv"

	"github.com/bdkiran/nolan/commitlog"
	"github.com/bdkiran/nolan/logger"
	"github.com/bdkiran/nolan/utils"
)

type Broker struct {
	Server    *Server
	topics    map[string]*commitlog.Commitlog
	directory string
}

func NewBroker(directory string) *Broker {
	server := NewServer()
	broker := Broker{
		Server:    server,
		directory: directory,
	}

	topics, err := broker.loadTopicSnapshot()
	if err != nil {
		logger.Warning.Println(err)
		topics = make(map[string]*commitlog.Commitlog)
	}

	broker.topics = topics
	return &broker
}

func (broker *Broker) Run() {
	for {
		var res []byte
		req := <-broker.Server.requestChan
		if req.MessageType == PRODUCER {
			res = broker.handleProduce(req)
		} else if req.MessageType == CONSUMER {
			res = broker.handleConsumer(req)
		} else {
			logger.Error.Println("Unknown request: ", req.MessageType)
			res = []byte{}
		}
		broker.Server.resposeChan <- &SocketMessage{
			MessageType: req.MessageType,
			topic:       req.topic,
			body:        res,
			conn:        req.conn,
		}
	}
}

func (broker *Broker) handleProduce(req *SocketMessage) []byte {
	requestMesage := req.body
	logger.Info.Println(string(requestMesage))

	commitlog := broker.topics[req.topic]

	err := commitlog.Append(requestMesage)
	if err != nil {
		logger.Error.Println(err)
	}
	socketMsg := utils.GetSocketBytes([]byte("AWK"))
	return socketMsg
}

func (broker *Broker) handleConsumer(req *SocketMessage) []byte {
	offset, err := strconv.Atoi(string(req.body))
	if err != nil {
		logger.Error.Println("Message problem: ", string(req.body), err)
	}
	commitlog := broker.topics[req.topic]
	logger.Info.Println(offset)
	requestMesageBuffer, err := commitlog.Read(offset)
	if err != nil {
		if err.Error() == "offset out of bounds" {
			logger.Error.Println("No message: ", err)
			socketMsg := utils.GetSocketBytes([]byte("No Message"))
			return socketMsg

		}
		logger.Error.Println("Unexpected error: ", err)
		return []byte{}
	}
	socketMsg := utils.GetSocketBytes([]byte(requestMesageBuffer))
	return socketMsg
}

// func handleGetTopicMetadata() {

// }

func (broker *Broker) handleCreateTopicRequest(req *SocketMessage) []byte {
	err := broker.CreateTopic(req.topic)
	if err != nil {
		socketMsg := utils.GetSocketBytes([]byte("Error"))
		return socketMsg
	}
	socketMsg := utils.GetSocketBytes([]byte("Created"))
	return socketMsg
}

func (broker *Broker) handleDeleteTopicRequest(req *SocketMessage) []byte {
	err := broker.DeleteTopic(req.topic)
	if err != nil {
		socketMsg := utils.GetSocketBytes([]byte("Error"))
		return socketMsg
	}
	socketMsg := utils.GetSocketBytes([]byte("Created"))
	return socketMsg
}
