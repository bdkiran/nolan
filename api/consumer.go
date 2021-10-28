package api

import (
	"errors"
	"sync"
	"time"

	"github.com/bdkiran/nolan/broker"
	"github.com/bdkiran/nolan/utils"

	"github.com/bdkiran/nolan/logger"
)

type Consumer struct {
	nolanConn       *nolanConnection
	recievedMsgChan chan *broker.Message
	msgChanState    chan bool
	pollTimeout     int
	waitTime        int
	messageQueSync  sync.Mutex
	executing       bool
	offset          int
}

func NewConsumer(topic string, offset int) (*Consumer, error) {
	nolanClient, err := createClient(CONSUMER, topic)
	if err != nil {
		logger.Error.Println(err)
		return nil, err
	}

	consumer := &Consumer{
		nolanConn:       nolanClient,
		recievedMsgChan: make(chan *broker.Message, 256),
		msgChanState:    make(chan bool),
		pollTimeout:     10,
		waitTime:        1,
		executing:       false,
		offset:          offset,
	}
	return consumer, nil
}

func (consumer *Consumer) Consume() (*broker.Message, error) {
	logger.Info.Println("Consume called")
	if !consumer.executing {
		go consumer.RecieveMessages()
	}
	state, ok := <-consumer.msgChanState
	if !ok {
		return nil, errors.New("connection ended")
	}
	if !state {
		logger.Info.Println(state)
		return nil, errors.New("no new messages")
	}
	msg := <-consumer.recievedMsgChan
	return msg, nil
}

func (consumer *Consumer) RecieveMessages() {
	logger.Info.Println("Recieve messages called")
	consumer.messageQueSync.Lock()
	defer consumer.messageQueSync.Unlock()

	err := consumer.nolanConn.createConnection(consumer.offset)
	if err != nil {
		logger.Error.Println(err)
		return
	}

	consumer.executing = true
	pollTimeoutDuration := time.Duration(consumer.pollTimeout) * time.Second
	waitTimeDuration := time.Duration(consumer.waitTime) * time.Second
	pollTimer := time.NewTimer(pollTimeoutDuration)

	for {
		select {
		case <-pollTimer.C:
			logger.Warning.Println("Consumer timeout hit. Closing Connection.")
			consumer.nolanConn.socketConnection.Close()
			consumer.msgChanState <- false
			consumer.executing = false
			return
		default:
			msg, err := utils.GetSocketMessage(consumer.nolanConn.socketConnection)
			if err != nil {
				logger.Error.Println(err)
				consumer.nolanConn.socketConnection.Close()
				consumer.msgChanState <- false
				consumer.executing = false
				return
			}

			srvMessageString := string(msg)
			if srvMessageString == "No Message" {
				consumer.msgChanState <- false
				logger.Info.Println("Server thing:", srvMessageString)
				retryMsg := utils.GetSocketBytes([]byte("RETRY"))
				consumer.nolanConn.socketConnection.Write(retryMsg)
				time.Sleep(waitTimeDuration)
			} else {
				mt, err := broker.Decode(msg)
				if err != nil {
					logger.Error.Fatalln(err)
				}
				consumer.recievedMsgChan <- &mt
				consumer.msgChanState <- true
				consumer.offset++
				awkMsg := utils.GetSocketBytes([]byte("AWK"))
				consumer.nolanConn.socketConnection.Write(awkMsg)
				pollTimer.Reset(pollTimeoutDuration)
			}
		}
	}
}
