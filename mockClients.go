/*
Alot of the functions here should be move to an api folder, as they can be reused to build clients.
Mock should be more of an e2e test to verify that things to break
*/
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/bdkiran/nolan/broker"
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

func NewProducer() {
	nolanClient, err := CreateClient(Producer)
	if err != nil {
		logger.Error.Println(err)
		return
	}

	err = nolanClient.CreateConnection()
	if err != nil {
		logger.Error.Println(err)
		return
	}

	i := 0
	for {
		key := []byte(fmt.Sprintf("Key %d", i))
		value := []byte(fmt.Sprintf("Value %d", i))

		message := broker.Message{
			Timestamp: time.Now(),
			Key:       key,
			Value:     value,
		}

		err = nolanClient.ProduceMessage(message)
		if err != nil {
			logger.Error.Println(err)
			return
		}
		time.Sleep(1 * time.Second)
		i++
	}
}

func NewConsumer() {
	nolanClient, err := CreateClient(Consumer)
	if err != nil {
		logger.Error.Println(err)
		return
	}

	err = nolanClient.CreateConnection()
	if err != nil {
		logger.Error.Println(err)
		return
	}
	nolanClient.ConsumeMessages(10, 1)
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
	//defer conn.Close()
}

func (nolanConn *nolanConnection) CreateConnection() error {
	conectionString := fmt.Sprintf("%s:%s\n", nolanConn.action, nolanConn.topic)
	//Establish connection
	_, err := nolanConn.socketConnection.Write([]byte(conectionString))
	if err != nil {
		nolanConn.socketConnection.Close()
		logger.Error.Fatal(err)
		return err
	}

	reply := make([]byte, 128)

	numberOfBytes, err := nolanConn.socketConnection.Read(reply)
	if err != nil {
		nolanConn.socketConnection.Close()
		logger.Error.Fatal(err)
		return err
	}
	//trim the buffer chars for comparison
	reply = reply[:numberOfBytes]

	if string(reply) != conectionString {
		nolanConn.socketConnection.Close()
		return errors.New("did not recieve correct connection message")
	}
	logger.Info.Println("Broker response: ", string(reply))
	return nil
}

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

func (nolanConn *nolanConnection) ConsumeMessages(pollTimeout int, waitTime int) {
	pollTimeoutDuration := time.Duration(pollTimeout) * time.Second
	waitTimeDuration := time.Duration(waitTime) * time.Second

	timerThing := time.NewTimer(pollTimeoutDuration)

	var i int
	for {
		i++
		select {
		case <-timerThing.C:
			logger.Warning.Println("Consumer timeout hit. Closing Connection.")
			nolanConn.socketConnection.Close()
			return
		default:
			size, err := getSocketMessageSize(nolanConn.socketConnection)
			if err != nil {
				logger.Error.Println(err)
				nolanConn.socketConnection.Close()
				return
			}
			b := make([]byte, size)
			if _, err = nolanConn.socketConnection.Read(b); err != nil {
				logger.Error.Println("Client left. Unable to read message.", err)
				nolanConn.socketConnection.Close()
				return
			}

			srvMessageString := string(b)
			if srvMessageString == "No Message" {
				logger.Info.Println("Server thing:", srvMessageString)
				retryMsg := getSocketBytes([]byte("RETRY"))
				nolanConn.socketConnection.Write(retryMsg)
				time.Sleep(waitTimeDuration)
			} else {
				mt, err := broker.Decode(b)
				if err != nil {
					logger.Error.Fatalln(err)
				}
				logger.Info.Println(mt.Timestamp)
				logger.Info.Println(string(mt.Key))
				logger.Info.Println(string(mt.Value))

				awkMsg := getSocketBytes([]byte("AWK"))
				nolanConn.socketConnection.Write(awkMsg)
				timerThing.Reset(pollTimeoutDuration)
			}
		}
	}
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
