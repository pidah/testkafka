package storage

import (
	"fmt"
	"gopkg.in/couchbase/gocb.v1"
)

type Couchbase struct {
	key string
	obj string
}

func (c *Couchbase) Write() {

	cluster, _ := gocb.Connect("couchbase://localhost")
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: "Administrator",
		Password: "peteridah",
	})
	bucket, _ := cluster.OpenBucket("example", "")
	_, err := bucket.Upsert(c.key, c.obj, 0)
	check(err)
	fmt.Printf("payload for key %s successfully stored in database\n", c.key)
}
