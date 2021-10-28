package api

import (
	"errors"

	"github.com/bdkiran/nolan/broker"
	"github.com/bdkiran/nolan/logger"
	"github.com/bdkiran/nolan/utils"
)

type Producer struct {
	nolanConn *nolanConnection
}

func NewProducer(topic string) (*Producer, error) {
	nolanClient, err := createClient(PRODUCER, topic)
	if err != nil {
		logger.Error.Println(err)
		return nil, err
	}

	err = nolanClient.createConnection(0)
	if err != nil {
		logger.Error.Println(err)
		return nil, err
	}

	producer := &Producer{
		nolanConn: nolanClient,
	}

	return producer, nil

}

func (producer *Producer) ProduceMessage(message broker.Message) error {
	msg, _ := message.Encode()
	fullMsg := utils.GetSocketBytes(msg)

	_, err := producer.nolanConn.socketConnection.Write(fullMsg)
	if err != nil {
		producer.nolanConn.socketConnection.Close()
		logger.Error.Fatal(err)
		return err
	}

	replyMsg, err := utils.GetSocketMessage(producer.nolanConn.socketConnection)
	if err != nil {
		logger.Error.Println(err)
		producer.nolanConn.socketConnection.Close()
		return err
	}
	//check the response....
	if string(replyMsg) != "AWK" {
		producer.nolanConn.socketConnection.Close()
		return errors.New("did not recieve correct awk message")
	}
	logger.Info.Println("Broker response: ", string(replyMsg))
	return nil
}
