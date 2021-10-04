package main

import (
	"flag"

	"github.com/bdkiran/nolan/broker"
	"github.com/bdkiran/nolan/commitlog"
	logger "github.com/bdkiran/nolan/utils"
)

func main() {
	logger.LoggerInit(false)
	mockClient := flag.String("client", "", "Flag to start the test producer or consumer")
	debugPtr := flag.Bool("debug", false, "Debug the commitlog by reading it")

	flag.Parse()

	logger.Info.Println(*mockClient)
	if *mockClient == "producer" {
		go ProducerClient()
	} else if *mockClient == "subscriber" {
		go consumerClient()
	}

	logger.Info.Println(*debugPtr)
	if *debugPtr {
		Debug()
		return
	}

	RunBroker()
}

func RunBroker() {
	finish := make(chan bool)
	broker := broker.NewBroker()
	broker.CreateTopic("topic1", "logs/partition0")
	go broker.Server.StartServer()
	go broker.Run()
	<-finish
}

func Debug() {
	logger.LoggerInit(false)
	logger.Info.Println("Nolan Starting up...")
	cl, _ := commitlog.New("logs/partition0")
	// cl.Append([]byte("We made it"))
	// cl.Append([]byte("Dont hate it"))
	// cl.Append([]byte("Another test"))
	//cl.ReadLatestEntry()
	cl.ReadAll()
}
