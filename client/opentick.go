package client

import (
	"encoding/binary"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"log"
	"net"
	"strconv"
	"sync"
)

type Future interface {
	Got() ([][]interface{}, error)
}

type future struct {
	token int
}

// not thread-safe
type Connection interface {
	Execute(sql string) (Future, error)
	Close()
}

type connection struct {
	conn         net.Conn
	tokenCounter int
	prepared     map[string]int
}

func Connect(host string, port int, dbName string) (ret Connection, err error) {
	conn, err := net.Dial("tcp", host+":"+strconv.FormatInt(int64(port)))
	if err != nil {
		return
	}
	c := connection{conn: conn}
	go recv(conn)
	ret = c
	return
}

func (self *connection) Execute(sql string, args ...interface{}) (ret Future, err error) {
	if len(args) > 0 {
		token := self.tokenCounter
		self.tokenCounter++
		cmd := []interface{}{token, "prepare", sql}
	}
	token := self.tokenCounter
	self.tokenCounter++
	return
}

func (self *connection) send() error {
}

func recv(c connection) {
	defer func() {
		log.Println("reading thread ended,", c.conn.RemoteAddr())
	}()
	for {
		var head [4]byte
		tmp := head[:4]
		for n, err := c.conn.Read(tmp); n < len(tmp); {
			tmp = tmp[n:]
			if err != nil {
				log.Println(err)
				c.conn.Close()
				return
			}
		}
		n := binary.LittleEndian.Uint32(head[:])
		if n == 0 {
			continue
		}
		body := make([]byte, n)
		tmp = body
		for n, err := c.conn.Read(tmp); n < len(tmp); {
			tmp = tmp[n:]
			if err != nil {
				log.Println(err)
				c.conn.Close()
				return
			}
		}
		var data []interface{}
		var err error
		err = bson.Unmarshal(body, &data)
		if err != nil {
			log.Println(err)
			c.conn.Close()
			return
		}
	}
}
