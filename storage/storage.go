package storage

import (
	"encoding/json"
	"log"
)

func storePayload(data string) {
	var objmap map[string]json.RawMessage
	//	var unescapedJson []byte
	//	enc := json.NewEncoder(os.Stdout)
	err := json.Unmarshal([]byte(data), &objmap)
	if err != nil {
		log.Fatal(err)
	}

	couchbase(objmap)
	storeUnloadPayload(objmap)
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
