package broker

import (
	"encoding/gob"
	"os"

	"github.com/bdkiran/nolan/commitlog"
	"github.com/bdkiran/nolan/logger"
)

type topic struct {
	TopicName      string
	TopicDirectory string
}

func (broker *Broker) CreateTopic(topicName string, directory string) error {
	cl, err := commitlog.New(directory)
	if err != nil {
		logger.Error.Println("Unable to initilize commitlog", err)
		return err
	}
	broker.topics[topicName] = cl
	logger.Info.Printf("Created %s topic successfully\n", topicName)
	go broker.takeTopicSnapshot() //Should this be in a go routine?
	return nil
}

func (broker *Broker) GetTopics() []string {
	allTopics := []string{}
	for topicName := range broker.topics {
		allTopics = append(allTopics, topicName)
	}
	return allTopics
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

	file, err := os.OpenFile("topics.gob", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
	file, err := os.Open("topics.gob")
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
