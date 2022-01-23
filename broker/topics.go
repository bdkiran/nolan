package broker

import (
	"encoding/gob"
	"errors"
	"os"

	"github.com/bdkiran/nolan/commitlog"
	"github.com/bdkiran/nolan/logger"
	"github.com/bdkiran/nolan/utils"
)

type topic struct {
	TopicName      string
	TopicDirectory string
}

func (broker *Broker) GetTopics() []string {
	allTopics := []string{}
	for topicName := range broker.topics {
		allTopics = append(allTopics, topicName)
	}
	return allTopics
}

func (broker *Broker) CreateTopic(topicName string) error {
	topicDirectory := broker.directory + "/" + utils.RandomString(5)
	logger.Info.Println("Creating new directory: ", topicDirectory)
	cl, err := commitlog.New(topicDirectory)
	if err != nil {
		logger.Error.Println("Unable to initilize commitlog", err)
		return err
	}
	broker.topics[topicName] = cl
	logger.Info.Printf("Created %s topic successfully\n", topicName)
	err = broker.takeTopicSnapshot() //Should this be in a go routine?
	if err != nil {
		logger.Error.Println("Unable to take a snapsnot", err)
		return err
	}
	return nil
}

func (broker *Broker) DeleteTopic(topic string) error {
	_, ok := broker.topics[topic]
	if ok {
		delete(broker.topics, topic)
	} else {
		return errors.New("topic does not exist, unable to delete")
	}
	logger.Info.Printf("Topic %s has been deleted", topic)
	err := broker.takeTopicSnapshot() //Should this be in a go routine?
	if err != nil {
		logger.Error.Println("Unable to take a snapsnot", err)
		return err
	}
	return nil
}

func (broker *Broker) takeTopicSnapshot() error {
	var snapTopics []topic

	for topicName, commitlog := range broker.topics {
		snapTopic := topic{
			TopicName:      topicName,
			TopicDirectory: commitlog.GetPath(),
		}

		snapTopics = append(snapTopics, snapTopic)
	}
	snapshotPath := broker.directory + "/topics.gob"

	file, err := os.OpenFile(snapshotPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Warning.Println(err)
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(snapTopics)
	if err != nil {
		logger.Warning.Println(err)
		return err
	}
	return nil
}

func (broker *Broker) loadTopicSnapshot() (map[string]*commitlog.Commitlog, error) {
	var snapTopics []topic
	decodedMap := make(map[string]*commitlog.Commitlog)

	snapshotPath := broker.directory + "/topics.gob"

	file, err := os.Open(snapshotPath)
	if err != nil {
		logger.Warning.Println(err)
		return decodedMap, err
	}
	decoder := gob.NewDecoder(file)

	err = decoder.Decode(&snapTopics)
	if err != nil {
		logger.Warning.Println(err)
		return decodedMap, err
	}

	logger.Info.Println(snapTopics)

	for _, snapTopic := range snapTopics {
		cl, err := commitlog.New(snapTopic.TopicDirectory)
		if err != nil {
			logger.Error.Println("Unable to initilize commitlog", err)
			return decodedMap, err
		}
		decodedMap[snapTopic.TopicName] = cl
		logger.Info.Printf("Initialized %s topic successfully\n", snapTopic.TopicName)
	}

	return decodedMap, nil
}
