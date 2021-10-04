package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"

	logger "github.com/bdkiran/nolan/utils"
)

type Message struct {
	Title string
	Body  string
}

func ProducerClient() {
	time.Sleep(5 * time.Second)
	logger.Info.Println("Creating connection..")
	conn, err := net.Dial("tcp", "127.0.0.1:6969")
	if err != nil {
		logger.Error.Fatal(err)
	}
	defer conn.Close()

	//Build our connection string
	topic := "topic1"
	conectionString := fmt.Sprintf("PRODUCER:%s\n", topic)
	//Establish connection
	_, err = conn.Write([]byte(conectionString))
	if err != nil {
		logger.Error.Fatal(err)
	}

	reply := make([]byte, 1024)

	_, err = conn.Read(reply)
	if err != nil {
		logger.Error.Fatal(err)
	}
	logger.Info.Println("Broker response: ", string(reply))

	i := 0
	for {
		title := fmt.Sprintf("Message %d", i)
		body := fmt.Sprintf("Body %d", i)

		m := Message{title, body}
		messageBuffer, err := json.Marshal(m)
		if err != nil {
			logger.Error.Fatalln(err)
		}

		messageBuffer = append(messageBuffer, "\n"...)

		_, err = conn.Write(messageBuffer)
		if err != nil {
			logger.Error.Fatal(err)
		}

		reply := make([]byte, 1024)

		_, err = conn.Read(reply)
		if err != nil {
			logger.Error.Fatal(err)
		}

		logger.Info.Println("Broker response: ", string(reply))
		i++
		time.Sleep(1 * time.Second)
	}
}

func consumerClient() {
	time.Sleep(5 * time.Second)
	logger.Info.Println("Creating connection..")
	conn, err := net.Dial("tcp", "127.0.0.1:6969")
	if err != nil {
		logger.Error.Fatal(err)
	}
	defer conn.Close()

	//Establish connection
	topic := "topic1"
	conectionString := fmt.Sprintf("CONSUMER:%s\n", topic)
	_, err = conn.Write([]byte(conectionString))
	if err != nil {
		logger.Error.Fatal(err)
	}

	reply := make([]byte, 1024)

	_, err = conn.Read(reply)
	if err != nil {
		logger.Error.Fatal(err)
	}
	logger.Info.Println("Broker response: ", string(reply))

	for {
		buffer, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			logger.Warning.Println("Server left.", err)
			conn.Close()
			return
		}
		srvMessage := string(buffer[:len(buffer)-1])
		logger.Info.Println("Server message:", srvMessage)
		conn.Write([]byte("AWK\n"))
	}
}
