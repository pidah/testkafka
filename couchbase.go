package main

import (
	"encoding/json"
	"fmt"
	"gopkg.in/couchbase/gocb.v1"
)

func couchbase(obj map[string]json.RawMessage) {

	key := trimQuote(string(obj["cookie"])) + "::" + trimQuote(string(obj["ptoken"])) + "::" + string(obj["sequenceId"])

	cluster, _ := gocb.Connect("couchbase://localhost")
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: "Administrator",
		Password: "peteridah",
	})
	bucket, _ := cluster.OpenBucket("example", "")
	_, err := bucket.Upsert(key, obj, 0)
	_, err = fmt.Println(&obj)
	check(err)
}
