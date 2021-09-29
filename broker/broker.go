package broker

import (
	"strconv"

	"github.com/bdkiran/nolan/commitlog"
	logger "github.com/bdkiran/nolan/utils"
)

// These should not be hard coded, pass these in as a configuration
const (
	connHost    = "127.0.0.1"
	connPort    = 6969
	connType    = "tcp"
	clDirectory = "logs/partition0"
)

type Broker struct {
	Server *Server
	topics map[string]*commitlog.Commitlog
}

// type Topic struct {
// 	TopicName string
// 	commitlog *commitlog.Commitlog
// }

func NewBroker() *Broker {
	server := NewServer()
	broker := Broker{
		Server: server,
		topics: make(map[string]*commitlog.Commitlog),
	}
	return &broker
}

func (broker *Broker) CreateTopic(topicName string, directory string) error {
	cl, err := commitlog.New(directory)
	if err != nil {
		logger.Error.Println("Unable to initilize commitlog", err)
		return err
	}
	broker.topics[topicName] = cl
	return nil
}

func (broker *Broker) Run() {
	for {
		var res []byte
		req := <-broker.Server.requestChan
		if req.requestType == "PRODUCE" {
			res = broker.handleProduce(req)
		} else if req.requestType == "CONSUME" {
			res = broker.handleConsumer(req)
		} else {
			logger.Error.Println("Unknown request: ", req.requestType)
			res = []byte{}
		}
		broker.Server.resposeChan <- &ReMessage{
			requestType: req.requestType,
			topic:       req.topic,
			body:        res,
			conn:        req.conn,
		}
	}
}

func (broker *Broker) handleProduce(req *ReMessage) []byte {
	requestMesage := req.body
	logger.Info.Println(string(requestMesage))

	commitlog := broker.topics[req.topic]

	err := commitlog.Append(requestMesage)
	if err != nil {
		logger.Error.Println(err)
	}
	return []byte("AWK\n")
}

func (broker *Broker) handleConsumer(req *ReMessage) []byte {
	offset, err := strconv.Atoi(string(req.body))
	if err != nil {
		logger.Error.Println("Message problem: ", string(req.body), err)
	}
	commitlog := broker.topics[req.topic]
	requestMesageBuffer, err := commitlog.Read(offset)
	if err != nil {
		logger.Error.Println("No message: ", err)
		return []byte{}
	}
	requestMesageBuffer = append(requestMesageBuffer, "\n"...)
	return requestMesageBuffer
}
