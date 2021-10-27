package api

import (
	"errors"
	"fmt"
	"net"

	"github.com/bdkiran/nolan/logger"
	"github.com/bdkiran/nolan/utils"
)

type actionType string

const (
	PRODUCER actionType = "PRODUCER"
	CONSUMER actionType = "CONSUMER"
)

type nolanConnection struct {
	address          string
	port             int
	action           actionType
	topic            string
	socketConnection net.Conn
}

func createClient(aType actionType, topic string) (*nolanConnection, error) {
	nolanConn := nolanConnection{
		address: "127.0.0.1",
		port:    6969,
		action:  aType,
		topic:   topic,
	}
	return &nolanConn, nil
}

func (nolanConn *nolanConnection) createConnection() error {
	dialConn := fmt.Sprintf("%s:%d", nolanConn.address, nolanConn.port)
	logger.Info.Println("Creating connection to ", dialConn)
	conn, err := net.Dial("tcp", dialConn)
	if err != nil {
		logger.Error.Fatal(err)
		return err
	}
	nolanConn.socketConnection = conn

	conectionString := fmt.Sprintf("%s:%s", nolanConn.action, nolanConn.topic)
	connectionStringBytes := utils.GetSocketBytes([]byte(conectionString))

	//Establish connection
	_, err = nolanConn.socketConnection.Write(connectionStringBytes)
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
