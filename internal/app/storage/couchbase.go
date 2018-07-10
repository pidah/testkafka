package storage

import (
	"fmt"
	"gopkg.in/couchbase/gocb.v1"
)

func couchbase(key, obj string) {

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
