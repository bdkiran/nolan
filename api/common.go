package api

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"

	logger "github.com/bdkiran/nolan/utils"
)

type actionType string

const (
	Producer actionType = "PRODUCER"
	Consumer actionType = "CONSUMER"
)

type nolanConnection struct {
	address          string
	port             int
	action           actionType
	topic            string
	socketConnection net.Conn
}

func CreateClient(aType actionType) (*nolanConnection, error) {
	nolanConn := nolanConnection{
		address: "127.0.0.1",
		port:    6969,
		action:  aType,
		topic:   "topic1",
	}

	dialConn := fmt.Sprintf("%s:%d", nolanConn.address, nolanConn.port)
	logger.Info.Println("Creating connection to ", dialConn)
	conn, err := net.Dial("tcp", dialConn)
	if err != nil {
		logger.Error.Fatal(err)
		return nil, err
	}
	nolanConn.socketConnection = conn
	return &nolanConn, nil
}

func (nolanConn *nolanConnection) CreateConnection() error {
	conectionString := fmt.Sprintf("%s:%s", nolanConn.action, nolanConn.topic)
	connectionStringBytes := getSocketBytes([]byte(conectionString))
	//Establish connection
	_, err := nolanConn.socketConnection.Write(connectionStringBytes)
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

//TODO: Create a generic library and look at reducing more code
func getSocketMessageSize(conn net.Conn) (uint32, error) {
	p := make([]byte, 4)
	_, err := conn.Read(p)
	if err != nil {
		return uint32(0), err
	}
	size := binary.LittleEndian.Uint32(p)
	return size, nil
}

//TODO: Create a generic library and look at reducing more code
func getSocketBytes(msg []byte) []byte {
	fullMsg := make([]byte, 4)
	binary.LittleEndian.PutUint32(fullMsg, uint32(len(msg)))
	fullMsg = append(fullMsg, msg...)
	return fullMsg
}
