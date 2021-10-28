package main

import (
	"fmt"
	"time"

	"github.com/bdkiran/nolan/api"
	"github.com/bdkiran/nolan/broker"
	"github.com/bdkiran/nolan/logger"
)

func ProduceMessages() {
	producer, err := api.NewProducer("topic1")
	if err != nil {
		logger.Error.Println(err)
		return
	}

	i := 0
	for {
		key := []byte{}
		value := []byte(fmt.Sprintf("Value %d", i))

		message := broker.Message{
			Timestamp: time.Now(),
			Key:       key,
			Value:     value,
		}

		err = producer.ProduceMessage(message)
		if err != nil {
			logger.Error.Println(err)
			return
		}
		time.Sleep(1 * time.Second)
		i++
	}
}

func ConsumeMessages() {
	consumer, err := api.NewConsumer("topic1", 0)
	if err != nil {
		logger.Error.Println(err)
		return
	}
	for {
		msg, err := consumer.Consume()
		if err != nil {
			logger.Warning.Println(err)
			continue
		}
		logger.Info.Printf("%v, %s, %s\n", msg.Timestamp, msg.Key, msg.Value)
	}

}
