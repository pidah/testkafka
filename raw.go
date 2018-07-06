package main

import (
	//	"bytes"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/couchbase/gocb.v1"
	//	"io/ioutil"
	//	"bufio"
	//	"io"
	"log"
	//	"os"
)

func main() {
	receivefromkafka()
}

func receivefromkafka() {
	fmt.Println("Start receiving from Kafka")
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "group-id-1",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"scrubbed"}, nil)

	for {
		msg, err := c.ReadMessage(-1)

		if err == nil {
			fmt.Printf("Received from Kafka %s: %s\n", msg.TopicPartition, string(msg.Value))
			job := string(msg.Value)
			savetodb(job)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}

	c.Close()

}

func savetodb(data string) {
	var objmap map[string]json.RawMessage
	//	var unescapedJson []byte
	//	enc := json.NewEncoder(os.Stdout)
	err := json.Unmarshal([]byte(data), &objmap)
	if err != nil {
		log.Fatal(err)
	}

	key := trimQuote(string(objmap["cookie"])) + "::" + trimQuote(string(objmap["ptoken"])) + "::" + string(objmap["sequenceId"])

	cluster, _ := gocb.Connect("couchbase://localhost")
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: "Administrator",
		Password: "peteridah",
	})
	bucket, _ := cluster.OpenBucket("example", "")
	_, err = bucket.Upsert(key, objmap, 0)
	_, err = fmt.Println(&objmap)
	if trimQuote(string(objmap["action"])) == "UNLOAD" {
		saveJobToKafka(objmap, "unload")
	}

	check(err)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func trimQuote(s string) string {
	if len(s) > 0 && s[0] == '"' {
		s = s[1:]
	}
	if len(s) > 0 && s[len(s)-1] == '"' {
		s = s[:len(s)-1]
	}
	return s
}
