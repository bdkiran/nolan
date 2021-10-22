package api

import (
	"errors"

	"github.com/bdkiran/nolan/broker"
	"github.com/bdkiran/nolan/logger"
	"github.com/bdkiran/nolan/utils"
)

func (nolanConn *nolanConnection) ProduceMessage(message broker.Message) error {
	msg, _ := message.Encode()
	fullMsg := utils.GetSocketBytes(msg)

	_, err := nolanConn.socketConnection.Write(fullMsg)
	if err != nil {
		nolanConn.socketConnection.Close()
		logger.Error.Fatal(err)
		return err
	}

	replyMsg, err := utils.GetSocketMessage(nolanConn.socketConnection)
	if err != nil {
		logger.Error.Println(err)
		nolanConn.socketConnection.Close()
		return err
	}
	//check the response....
	if string(replyMsg) != "AWK" {
		nolanConn.socketConnection.Close()
		return errors.New("did not recieve correct awk message")
	}
	logger.Info.Println("Broker response: ", string(replyMsg))
	return nil
}
