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
	pollTimeout     int
	waitTime        int
	messageQueSync  sync.Once
}

func NewConsumer(topic string) (*Consumer, error) {
	nolanClient, err := createClient(CONSUMER, topic)
	if err != nil {
		logger.Error.Println(err)
		return nil, err
	}

	err = nolanClient.createConnection()
	if err != nil {
		logger.Error.Println(err)
		return nil, err
	}

	consumer := &Consumer{
		nolanConn:       nolanClient,
		recievedMsgChan: make(chan *broker.Message, 256),
		pollTimeout:     10,
		waitTime:        1,
	}
	return consumer, nil
}

func (consumer *Consumer) Consume() (*broker.Message, error) {
	go consumer.messageQueSync.Do(func() { consumer.RecieveMessages() })

	msg := <-consumer.recievedMsgChan
	if msg == nil {
		return nil, errors.New("channel closed, no new messages")
	}
	return msg, nil
}

func (consumer *Consumer) RecieveMessages() {
	pollTimeoutDuration := time.Duration(consumer.pollTimeout) * time.Second
	waitTimeDuration := time.Duration(consumer.waitTime) * time.Second

	pollTimer := time.NewTimer(pollTimeoutDuration)

	var i int
	for {
		i++
		select {
		case <-pollTimer.C:
			logger.Warning.Println("Consumer timeout hit. Closing Connection.")
			consumer.nolanConn.socketConnection.Close()
			close(consumer.recievedMsgChan)
			return
		default:
			msg, err := utils.GetSocketMessage(consumer.nolanConn.socketConnection)
			if err != nil {
				logger.Error.Println(err)
				consumer.nolanConn.socketConnection.Close()
				return
			}

			srvMessageString := string(msg)
			if srvMessageString == "No Message" {
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

				awkMsg := utils.GetSocketBytes([]byte("AWK"))
				consumer.nolanConn.socketConnection.Write(awkMsg)
				pollTimer.Reset(pollTimeoutDuration)
			}
		}
	}
}
