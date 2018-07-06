package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func saveJobToKafka(job map[string]json.RawMessage, topic string) {

	fmt.Println("save to kafka")
	fmt.Printf("save to kafka topic %s.\n", topic)

	jsonString, err := json.Marshal(job)

	jobString := string(jsonString)
	fmt.Print(jobString)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	// Produce messages to topic (asynchronously)
	//      topic := "scrubbed"
	for _, word := range []string{string(jobString)} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}
}
