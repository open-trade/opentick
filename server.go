package opentick

import (
	"encoding/binary"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"gopkg.in/mgo.v2/bson"
	"log"
	"math/rand"
	"net"
)

var defaultDBs []fdb.Transactor

var sNumDatabaseConn = 1

func getDB() fdb.Transactor {
	return defaultDBs[rand.Intn(sNumDatabaseConn)]
}

func StartServer(addr string, fdbClusterFile string, numDatabaseConn int) error {
	fdb.MustAPIVersion(FdbVersion)
	if numDatabaseConn > sNumDatabaseConn {
		sNumDatabaseConn = numDatabaseConn
	}
	defaultDBs = make([]fdb.Transactor, sNumDatabaseConn)
	for i := 0; i < sNumDatabaseConn; i++ {
		if fdbClusterFile == "" {
			defaultDBs[i] = fdb.MustOpenDefault()
		} else {
			// In the current release of fdb, the database name must be []byte("DB").
			defaultDBs[i] = fdb.MustOpen(fdbClusterFile, []byte("DB"))
		}
	}
	ln, err := net.Listen("tcp", addr)
	log.Println("Listening on " + addr)
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
		n := len(tmp)
		for n > 0 {
			n2, err := conn.Read(tmp)
			if err != nil {
				log.Println(err.Error(), "of connection", conn.RemoteAddr())
				return
			}
			tmp = tmp[n2:]
			n -= n2
		}
		bodyLen := binary.LittleEndian.Uint32(head[:])
		if bodyLen == 0 {
			continue
		}
		body := make([]byte, bodyLen)
		tmp = body
		n = len(tmp)
		for n > 0 {
			n2, err := conn.Read(tmp)
			if err != nil {
				log.Println(err.Error(), "of connection", conn.RemoteAddr())
				return
			}
			tmp = tmp[n2:]
			n -= n2
		}
		go func() {
			var data map[string]interface{}
			var err error
			var ok bool
			var ticker int
			var cmd string
			var sql string
			var preparedId int
			var ast *Ast
			var res interface{}
			var args []interface{}
			var exists bool
			var stmt interface{}
			err = bson.Unmarshal(body, &data)
			if err != nil {
				res = "Invalid bson: " + err.Error()
				goto reply
			}
			ticker, ok = data["0"].(int)
			if !ok {
				res = fmt.Sprint("Invalid ticker, expected int, got ", data["0"])
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
				if sql != "" {
					res, err = Execute(getDB(), dbName, sql, args)
				} else {
					res, err = ExecuteStmt(getDB(), stmt, args)
				}
				if err != nil {
					res = err.Error()
				}
			} else if cmd == "batch" {
				if sql != "" {
					res = "Batch command must be prepared first"
					goto reply
				}
				stmt2, ok2 := stmt.(insertStmt)
				if !ok2 {
					res = "Only batch insert supported"
					goto reply
				}
				argsArray := make([][]interface{}, len(args))
				for i, a := range args {
					var a2 []interface{}
					a2, ok := a.([]interface{})
					if !ok {
						res = "Arguments must be array of array"
						goto reply
					}
					if i != 0 && len(a2) != len(argsArray[0]) {
						res = "All array must the same size"
						goto reply
					}
					argsArray[i] = a2
				}
				err = BatchInsert(getDB(), &stmt2, argsArray)
				if err != nil {
					res = err.Error()
				}
			} else if cmd == "prepare" {
				ast, err = Parse(sql)
				if err != nil {
					res = err.Error()
					goto reply
				}
				res, err = Resolve(getDB(), dbName, ast)
				if err != nil {
					res = err.Error()
					goto reply
				}
				prepared = append(prepared, res)
				res = len(prepared) - 1
			} else if cmd == "use" {
				dbName = sql
				exists, err = HasDatabase(getDB(), dbName)
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
			reply(ticker, res, ch)
		}()
	}
}

func reply(ticker int, res interface{}, ch chan []byte) {
	defer func() {
		if err := recover(); err != nil {
			// log.Println(err)
		}
	}()
	data, err := bson.Marshal(map[string]interface{}{"0": ticker, "1": res})
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
				msg = msg[n2:]
				n -= n2
			}
		}
	}
}
