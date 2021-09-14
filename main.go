package main

import (
	"fmt"
	"net"
	"time"

	"github.com/bdkiran/nolan/commitlog"
	logger "github.com/bdkiran/nolan/utils"
)

func producerClient() {
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

func consumerClinet() {

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

	i := 0
	for {
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

func main() {
	logger.LoggerInit(false)
	//go producer()
	another()
	//broker.Main()
}

func another() {
	logger.LoggerInit(false)
	logger.Info.Println("Nolan Starting up...")
	cl, _ := commitlog.New("logs/partition0")
	cl.Append([]byte("We made it"))
	cl.Append([]byte("Dont hate it"))
	cl.Append([]byte("Another test"))
	//cl.ReadLatestEntry()
	//cl.Read(0)
	cl.ReadAll()
}
