package main

import (
	"encoding/json"
	"gopkg.in/mgo.v2/bson"
	"log"
	"time"
)

func main() {
	value := []float64{99999999, 1.22222, 1.3222222, 1.422222}
	var values []interface{}
	for i := 0; i < 10; i++ {
		values = append(values, value)
	}
	data := map[string]interface{}{"0": "test", "1": 1, "2": values}
	var body []byte
	log.Print("json")
	now := time.Now()
	for i := 0; i < 100000; i++ {
		body, _ = json.Marshal(data)
	}
	log.Print("body size   ", len(body))
	log.Println("serialize  ", time.Now().Sub(now))
	now = time.Now()
	for i := 0; i < 100000; i++ {
		_ = json.Unmarshal(body, &data)
	}
	log.Println("deserialize", time.Now().Sub(now))

	log.Print("bson")
	now = time.Now()
	for i := 0; i < 100000; i++ {
		body, _ = bson.Marshal(data)
	}
	log.Print("body size   ", len(body))
	log.Println("serialize  ", time.Now().Sub(now))
	now = time.Now()
	for i := 0; i < 100000; i++ {
		_ = bson.Unmarshal(body, &data)
	}
	log.Println("deserialize", time.Now().Sub(now))
}
