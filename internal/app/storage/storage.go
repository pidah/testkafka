package storage

import (
	"encoding/json"
	"strconv"
)

type Identifier struct {
	Action     string
	Cookie     string
	Ptoken     string
	Token      string
	SequenceId int
}

// Storage interface that supports any database.
type Storage interface {
	Write()
}

// Generic function for concrete implementation of a database
func DbWrite(s Storage) {
	s.Write()
}

//Partially deserialize the JSON object to retrive Identifier fields only. Everywhere else we pass []bytes.

func storePayload(rawData []byte) {
	var i Identifier
	objmap := (*json.RawMessage)(&rawData)
	err := json.Unmarshal(*objmap, &i)

	check(err)

	// TODO ptoken to be dropped in the new version of the message format
	if i.Ptoken == "" {
		i.Ptoken = i.Token
	}

	sequenceId := strconv.Itoa(i.SequenceId)
	key := i.Cookie + "::" + i.Ptoken + "::" + sequenceId

	c := &Couchbase{key, string(rawData)}
	DbWrite(c)

	if i.Action == "UNLOAD" {
		producer(rawData, "unload")
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
