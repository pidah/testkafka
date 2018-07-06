package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
)

type Job struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Company     string `json:"company"`
	Salary      string `json:"salary"`
}

type Message struct {
	Salary string          `json:"salary"`
	Data   json.RawMessage `json:"data"`
}

func main() {

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", jobsPostHandler).Methods("POST")

	log.Fatal(http.ListenAndServe(":9090", router))

}

func jobsPostHandler(w http.ResponseWriter, r *http.Request) {
	var ingress map[string]json.RawMessage

	//Retrieve body from http request
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(b, &ingress)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	saveJobToKafka(ingress, "scrubbed")

	var m Message
	json.Unmarshal(b, &m)
	//es.Index(index, m.Type, m.ID, "", "", nil, m.Data, false)
	fmt.Println(m.Salary)
	//Convert job struct into json
	jsonString, err := json.Marshal(ingress)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	//Set content-type http header
	w.Header().Set("content-type", "application/json")

	//Send back data as response
	w.Write(jsonString)

}

func saveJobToKafka(job map[string]json.RawMessage, topic string) {

	fmt.Println("save to kafka")

	jsonString, err := json.Marshal(job)

	jobString := string(jsonString)
	fmt.Print(jobString)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	// Produce messages to topic (asynchronously)
	//	topic := "scrubbed"
	for _, word := range []string{string(jobString)} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}
}
