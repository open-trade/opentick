package opentick

import (
	"encoding/binary"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"gopkg.in/mgo.v2/bson"
	"log"
	"net"
	"strconv"
)

var defaultDB fdb.Transactor

func StartServer(ip string, port int) error {
	fdb.MustAPIVersion(FdbVersion)
	defaultDB = fdb.MustOpenDefault()
	ln, err := net.Listen("tcp", ip+":"+strconv.FormatInt(int64(port), 10))
	if err != nil {
		return err
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go handleConnection(conn)
	}
	return nil
}

type connection struct {
	ch   chan []byte
	conn net.Conn
}

func (self *connection) Send(msg []byte) {
	self.ch <- msg
}

func handleConnection(conn net.Conn) {
	log.Println("New connection from", conn.RemoteAddr())
	ch := make(chan []byte)
	defer func() {
		close(ch)
		conn.Close()
		log.Println("Closed connection from", conn.RemoteAddr())
	}()
	go writeToConnection(connection{ch, conn})
	var prepared []interface{}
	var dbName string
	for {
		var head [4]byte
		tmp := head[:4]
		for n, err := conn.Read(tmp); n < len(tmp); {
			tmp = tmp[n:]
			if err != nil {
				log.Println(err)
				return
			}
		}
		n := binary.LittleEndian.Uint32(head[:])
		if n == 0 {
			continue
		}
		body := make([]byte, n)
		tmp = body
		for n, err := conn.Read(tmp); n < len(tmp); {
			tmp = tmp[n:]
			if err != nil {
				log.Println(err)
				return
			}
		}
		var data map[string]interface{}
		var err error
		err = bson.Unmarshal(body, &data)
		if err != nil {
			log.Println(err)
			return
		}
		var ok bool
		var token int
		var cmd string
		var sql string
		var preparedId int
		var ast *Ast
		var res interface{}
		var args []interface{}
		var exists bool
		var stmt interface{}
		token, ok = data["0"].(int)
		if !ok {
			res = fmt.Sprint("Invalid token, expected int, got ", data["0"])
			goto reply
		}
		cmd, ok = data["1"].(string)
		if !ok {
			res = fmt.Sprint("Invalid command, exepcted string, got ", data["1"])
			goto reply
		}
		if len(data) > 3 && data["3"] != nil {
			args, ok = data["3"].([]interface{})
			if !ok {
				res = fmt.Sprint("Invalid arguments, expected array, got ", data["3"])
				goto reply
			}
		}
		sql, ok = data["2"].(string)
		if !ok {
			preparedId, ok = data["2"].(int)
			if !ok {
				res = fmt.Sprint("Invalid sql, expected string or int (prepared id), got ", data["2"])
				goto reply
			}
			if preparedId >= len(prepared) {
				res = fmt.Sprint("Invalid preparedId ", preparedId)
				goto reply
			}
			stmt = prepared[preparedId]
		} else if sql == "" {
			res = "Empty sql"
			goto reply
		}
		if cmd == "run" {
			go func() {
				if sql != "" {
					res, err = Execute(defaultDB, dbName, sql, args)
				} else {
					res, err = ExecuteStmt(defaultDB, stmt, args)
				}
				if err != nil {
					res = err.Error()
				}
				reply(token, res, ch)
			}()
			continue
		} else if cmd == "prepare" {
			ast, err = Parse(sql)
			if err != nil {
				res = err.Error()
				goto reply
			}
			res, err = Resolve(defaultDB, dbName, ast)
			if err != nil {
				res = err.Error()
				goto reply
			}
			prepared = append(prepared, res)
			res = len(prepared) - 1
		} else if cmd == "use" {
			dbName = sql
			exists, err = HasDatabase(defaultDB, dbName)
			if err != nil {
				res = err.Error()
				goto reply
			}
			if !exists {
				res = dbName + " does not exist"
			}
		} else {
			res = "Invalid command " + cmd
		}
	reply:
		reply(token, res, ch)
	}
}

func reply(token int, res interface{}, ch chan []byte) {
	data, err := bson.Marshal(map[string]interface{}{"0": token, "1": res})
	if err != nil {
		panic(err)
	}
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(data)))
	ch <- append(size[:], data...)
}

func writeToConnection(c connection) {
	defer func() {
		log.Println("Writing thread ended,", c.conn.RemoteAddr())
	}()
	for {
		select {
		case msg, ok := <-c.ch:
			if !ok {
				return
			}
			n := len(msg)
			for n > 0 {
				n2, err := c.conn.Write(msg)
				if err != nil {
					return
				}
				n -= n2
				msg = msg[n2:]
			}
		}
	}
}
