package main

import (
	"github.com/bdkiran/nolan/broker"
	"github.com/bdkiran/nolan/commitlog"
	logger "github.com/bdkiran/nolan/utils"
)

func main() {
	logger.LoggerInit(false)
	Debug()
	RunBroker()

	// //another()
	// server := broker.NewServer()
	// server.StartServer()
	// debug()
}

func RunBroker() {
	finish := make(chan bool)
	go broker.ProducerClient()
	//go broker.ConsumerClinet()
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
