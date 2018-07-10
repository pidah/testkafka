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

	couchbase(key, string(rawData))

	if i.Action == "UNLOAD" {
		producer(rawData, "unload")
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
