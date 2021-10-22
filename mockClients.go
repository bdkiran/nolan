package main

import (
	"fmt"
	"time"

	"github.com/bdkiran/nolan/api"
	"github.com/bdkiran/nolan/broker"
	"github.com/bdkiran/nolan/logger"
)

func NewProducer() {
	nolanClient, err := api.CreateClient(api.Producer)
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
	nolanClient, err := api.CreateClient(api.Consumer)
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
