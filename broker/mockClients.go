package broker

import (
	"bufio"
	"fmt"
	"net"
	"time"

	logger "github.com/bdkiran/nolan/utils"
)

func ProducerClient() {
	time.Sleep(5 * time.Second)
	logger.Info.Println("Creating connection..")
	conn, err := net.Dial("tcp", "127.0.0.1:6969")
	if err != nil {
		logger.Error.Fatal(err)
	}
	defer conn.Close()

	//Establish connection
	_, err = conn.Write([]byte("PRODUCER\n"))
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
		msg := fmt.Sprintf("Message: %d\n", i)

		_, err := conn.Write([]byte(msg))
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

func ConsumerClinet() {
	time.Sleep(5 * time.Second)
	logger.Info.Println("Creating connection..")
	conn, err := net.Dial("tcp", "127.0.0.1:6969")
	if err != nil {
		logger.Error.Fatal(err)
	}
	defer conn.Close()

	//Establish connection
	_, err = conn.Write([]byte("CONSUMER\n"))
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
