package api

import (
	"errors"

	"github.com/bdkiran/nolan/broker"
	logger "github.com/bdkiran/nolan/utils"
)

func (nolanConn *nolanConnection) ProduceMessage(message broker.Message) error {
	msg, _ := message.Encode()
	fullMsg := getSocketBytes(msg)

	_, err := nolanConn.socketConnection.Write(fullMsg)
	if err != nil {
		nolanConn.socketConnection.Close()
		logger.Error.Fatal(err)
		return err
	}

	size, err := getSocketMessageSize(nolanConn.socketConnection)
	if err != nil {
		logger.Error.Println(err)
		nolanConn.socketConnection.Close()
		return err
	}
	reply := make([]byte, size)
	if _, err := nolanConn.socketConnection.Read(reply); err != nil {
		nolanConn.socketConnection.Close()
		logger.Error.Fatal(err)
		return err
	}
	//check the response....
	if string(reply) != "AWK" {
		nolanConn.socketConnection.Close()
		return errors.New("did not recieve correct awk message")
	}
	logger.Info.Println("Broker response: ", string(reply))
	return nil
}
