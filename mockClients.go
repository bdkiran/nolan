package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	logger "github.com/bdkiran/nolan/utils"
)

type Message struct {
	Title string
	Body  string
}

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
		title := fmt.Sprintf("Message %d", i)
		body := fmt.Sprintf("Body %d", i)

		m := Message{title, body}
		messageBuffer, err := json.Marshal(m)
		if err != nil {
			logger.Error.Fatalln(err)
			return
		}
		err = nolanClient.ProduceMessage(messageBuffer)
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

	reply := make([]byte, 256)

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

func (nolanConn *nolanConnection) ProduceMessage(message []byte) error {
	logger.Info.Println(string(message))
	message = append(message, "\n"...)

	_, err := nolanConn.socketConnection.Write(message)
	if err != nil {
		nolanConn.socketConnection.Close()
		logger.Error.Fatal(err)
		return err
	}

	reply := make([]byte, 256)

	numberOfBytes, err := nolanConn.socketConnection.Read(reply)
	if err != nil {
		nolanConn.socketConnection.Close()
		logger.Error.Fatal(err)
		return err
	}
	//trim the buffer chars
	reply = reply[:numberOfBytes]
	//trim the newline
	reply = reply[:len(reply)-1]
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
			buffer, err := bufio.NewReader(nolanConn.socketConnection).ReadBytes('\n')
			if err != nil {
				logger.Warning.Println("Server left.", err)
				nolanConn.socketConnection.Close()
				return
			}
			srvMessage := string(buffer[:len(buffer)-1])
			if srvMessage == "No Message" {
				logger.Info.Println("Server thing:", srvMessage)
				nolanConn.socketConnection.Write([]byte("RETRY\n"))
				time.Sleep(waitTimeDuration)
			} else {
				logger.Info.Println("Server message:", srvMessage)
				nolanConn.socketConnection.Write([]byte("AWK\n"))
				timerThing.Reset(pollTimeoutDuration)
			}
		}
	}
}

// func producerClient(rate int) {
// 	time.Sleep(5 * time.Second)
// 	logger.Info.Println("Creating connection..")
// 	conn, err := net.Dial("tcp", "127.0.0.1:6969")
// 	if err != nil {
// 		logger.Error.Fatal(err)
// 	}
// 	defer conn.Close()

// 	//Build our connection string
// 	topic := "topic1"
// 	conectionString := fmt.Sprintf("PRODUCER:%s\n", topic)
// 	//Establish connection
// 	_, err = conn.Write([]byte(conectionString))
// 	if err != nil {
// 		logger.Error.Fatal(err)
// 	}

// 	reply := make([]byte, 1024)

// 	_, err = conn.Read(reply)
// 	if err != nil {
// 		logger.Error.Fatal(err)
// 	}
// 	logger.Info.Println("Broker response: ", string(reply))

// 	i := 0
// 	for {
// 		title := fmt.Sprintf("Message %d", i)
// 		body := fmt.Sprintf("Body %d", i)

// 		m := Message{title, body}
// 		messageBuffer, err := json.Marshal(m)
// 		if err != nil {
// 			logger.Error.Fatalln(err)
// 		}

// 		messageBuffer = append(messageBuffer, "\n"...)

// 		_, err = conn.Write(messageBuffer)
// 		if err != nil {
// 			logger.Error.Fatal(err)
// 		}

// 		reply := make([]byte, 1024)

// 		_, err = conn.Read(reply)
// 		if err != nil {
// 			logger.Error.Fatal(err)
// 		}

// 		logger.Info.Println("Broker response: ", string(reply))
// 		i++
// 		time.Sleep(time.Duration(rate) * time.Second)
// 	}
// }

// func consumerClient(timeout int, waitTime int) {
// 	time.Sleep(5 * time.Second)
// 	logger.Info.Println("Creating connection..")
// 	conn, err := net.Dial("tcp", "127.0.0.1:6969")
// 	if err != nil {
// 		logger.Error.Fatal(err)
// 	}
// 	defer conn.Close()

// 	//Establish connection
// 	topic := "topic1"
// 	conectionString := fmt.Sprintf("CONSUMER:%s\n", topic)
// 	_, err = conn.Write([]byte(conectionString))
// 	if err != nil {
// 		logger.Error.Fatal(err)
// 	}

// 	reply := make([]byte, 1024)

// 	_, err = conn.Read(reply)
// 	if err != nil {
// 		logger.Error.Fatal(err)
// 	}
// 	logger.Info.Println("Broker response: ", string(reply))

// 	timeoutDuration := time.Duration(timeout) * time.Second
// 	waitTimeDuration := time.Duration(waitTime) * time.Second

// 	timerThing := time.NewTimer(timeoutDuration)

// 	var i int
// 	for {
// 		i++
// 		select {
// 		case <-timerThing.C:
// 			conn.Close()
// 			return
// 		default:
// 			buffer, err := bufio.NewReader(conn).ReadBytes('\n')
// 			if err != nil {
// 				logger.Warning.Println("Server left.", err)
// 				conn.Close()
// 				return
// 			}
// 			srvMessage := string(buffer[:len(buffer)-1])
// 			if srvMessage == "No Message" {
// 				logger.Info.Println("Server thing:", srvMessage)
// 				conn.Write([]byte("RETRY\n"))
// 				time.Sleep(waitTimeDuration)
// 			} else {
// 				logger.Info.Println("Server message:", srvMessage)
// 				conn.Write([]byte("AWK\n"))
// 				timerThing.Reset(timeoutDuration)
// 			}
// 		}
// 	}
// }
