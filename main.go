package main

import (
	"github.com/bdkiran/nolan/broker"
	"github.com/bdkiran/nolan/commitlog"
	logger "github.com/bdkiran/nolan/utils"
)

func main() {
	logger.LoggerInit(false)
	//go broker.producer()
	//go broker.ConsumerClinet()
	//another()
	broker.Main()
}

func Another() {
	logger.LoggerInit(false)
	logger.Info.Println("Nolan Starting up...")
	cl, _ := commitlog.New("logs/partition0")
	// cl.Append([]byte("We made it"))
	// cl.Append([]byte("Dont hate it"))
	// cl.Append([]byte("Another test"))
	//cl.ReadLatestEntry()
	cl.ReadAll()
	cl.Read(0)
}
