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

func producerClient(rate int) {
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
		time.Sleep(time.Duration(rate) * time.Second)
	}
}

func consumerClient(timeout int) {
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

	// var i int
	// for stay, timeoutChan := true, time.After(time.Duration(timeout)*time.Second); stay; {
	// 	i++
	// 	select {
	// 	case <-timeoutChan:
	// 		conn.Close()
	// 		stay = false
	// 	default:
	// 		buffer, err := bufio.NewReader(conn).ReadBytes('\n')
	// 		if err != nil {
	// 			logger.Warning.Println("Server left.", err)
	// 			conn.Close()
	// 			return
	// 		}
	// 		srvMessage := string(buffer[:len(buffer)-1])
	// 		if srvMessage == "No Message" {
	// 			logger.Info.Println("Server thing:", srvMessage)
	// 			time.Sleep(1 * time.Second)
	// 			conn.Write([]byte("AWK\n"))
	// 			continue
	// 		}
	// 		logger.Info.Println("Server message:", srvMessage)
	// 		conn.Write([]byte("AWK\n"))
	// 	}
	// }

	timeoutDuration := time.Duration(timeout) * time.Second

	timerThing := time.NewTimer(timeoutDuration)

	var i int
	for {
		i++
		select {
		case <-timerThing.C:
			conn.Close()
			return
		default:
			buffer, err := bufio.NewReader(conn).ReadBytes('\n')
			if err != nil {
				logger.Warning.Println("Server left.", err)
				conn.Close()
				return
			}
			srvMessage := string(buffer[:len(buffer)-1])
			if srvMessage == "No Message" {
				logger.Info.Println("Server thing:", srvMessage)
				conn.Write([]byte("RETRY\n"))
				time.Sleep(1 * time.Second)
			} else {
				logger.Info.Println("Server message:", srvMessage)
				conn.Write([]byte("AWK\n"))
				timerThing.Reset(timeoutDuration)
			}
		}
	}

	// for {
	// 	buffer, err := bufio.NewReader(conn).ReadBytes('\n')
	// 	if err != nil {
	// 		logger.Warning.Println("Server left.", err)
	// 		conn.Close()
	// 		return
	// 	}
	// 	srvMessage := string(buffer[:len(buffer)-1])
	// 	logger.Info.Println("Server message:", srvMessage)
	// 	conn.Write([]byte("AWK\n"))
	// }
}
