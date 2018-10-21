package opentick

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"gopkg.in/mgo.v2/bson"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"sync"
)

var defaultDBs []fdb.Transactor

var sNumDatabaseConn = 1
var sMaxConcurrency = 100

func getDB() fdb.Transactor {
	return defaultDBs[rand.Intn(sNumDatabaseConn)]
}

func StartServer(addr string, fdbClusterFile string, numDatabaseConn, maxConcurrency int) error {
	log.SetOutput(os.Stdout)
	fdb.MustAPIVersion(FdbVersion)
	if numDatabaseConn > sNumDatabaseConn {
		sNumDatabaseConn = numDatabaseConn
	}
	log.Println("Number of fdb connections:", sNumDatabaseConn)
	if maxConcurrency > 0 {
		sMaxConcurrency = maxConcurrency
	}
	log.Println("Max concurrency of one connection:", sMaxConcurrency)
	defaultDBs = make([]fdb.Transactor, sNumDatabaseConn)
	for i := 0; i < sNumDatabaseConn; i++ {
		if fdbClusterFile == "" {
			defaultDBs[i] = fdb.MustOpenDefault()
		} else {
			// In the current release of fdb, the database name must be []byte("DB").
			defaultDBs[i] = fdb.MustOpen(fdbClusterFile, []byte("DB"))
		}
	}
	laddr, err1 := net.ResolveTCPAddr("tcp", addr)
	if err1 != nil {
		return err1
	}
	ln, err2 := net.ListenTCP("tcp", laddr)
	log.Println("Listening on " + addr)
	if err2 != nil {
		return err2
	}
	defer ln.Close()
	for {
		conn, err := ln.AcceptTCP()
		conn.SetNoDelay(true)
		if err != nil {
			return err
		}
		go handleConnection(conn)
	}
	return nil
}

type connection struct {
	ch     chan []byte
	conn   net.Conn
	store  [][]byte
	mutex  sync.Mutex
	cond   *sync.Cond
	closed bool
}

func (self *connection) Send(msg []byte) {
	self.ch <- msg
}

func handleConnection(conn net.Conn) {
	log.Println("New connection from", conn.RemoteAddr())
	client := connection{ch: make(chan []byte), conn: conn}
	client.cond = sync.NewCond(&client.mutex)
	defer client.close()
	go client.writeToConnection()
	go client.process()
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
		client.push(body)
	}
}

func reply(ticker int, res interface{}, ch chan []byte, useJson bool) {
	defer func() {
		if err := recover(); err != nil {
			// send on closed channel
		}
	}()
	var data []byte
	var err error
	if useJson {
		data, err = json.Marshal(map[string]interface{}{"0": ticker, "1": res})
	} else {
		data, err = bson.Marshal(map[string]interface{}{"0": ticker, "1": res})
	}
	if err != nil {
		reply(ticker, "Internal error: "+err.Error(), ch, useJson)
		return
	}
	if len(data) > math.MaxUint32 {
		reply(ticker, "Results too large", ch, useJson)
		return
	}
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(data)))
	ch <- append(size[:], data...)
}

func (c *connection) writeToConnection() {
	defer func() {
		log.Println("Writing thread ended from", c.conn.RemoteAddr())
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

func (self *connection) process() {
	var prepared []interface{}
	var mut sync.Mutex
	var dbName string
	var useJson bool
	var unfinished int32
	for {
		var body []byte
		self.mutex.Lock()
		for {
			if self.closed {
				log.Println("Process thread ended from", self.conn.RemoteAddr())
				self.mutex.Unlock()
				return
			} else if len(self.store) == 0 || unfinished > int32(sMaxConcurrency) {
				self.cond.Wait()
			} else {
				body = self.store[0]
				self.store = self.store[1:]
				break
			}
		}
		unfinished++
		self.mutex.Unlock()
		go func() {
			defer func() {
				self.mutex.Lock()
				unfinished--
				self.cond.Signal()
				self.mutex.Unlock()
			}()
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
			if useJson {
				err = json.Unmarshal(body, &data)
			} else {
				err = bson.Unmarshal(body, &data)
			}
			if err != nil {
				if string(body) == "protocol=json" {
					useJson = true
					return
				}
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
				mut.Lock()
				if preparedId >= len(prepared) {
					mut.Unlock()
					res = fmt.Sprint("Invalid preparedId ", preparedId)
					goto reply
				}
				stmt = prepared[preparedId]
				mut.Unlock()
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
				mut.Lock()
				prepared = append(prepared, res)
				res = len(prepared) - 1
				mut.Unlock()
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
			reply(ticker, res, self.ch, useJson)
		}()
	}
}

func (self *connection) close() {
	close(self.ch)
	self.conn.Close()
	self.mutex.Lock()
	self.closed = true
	self.cond.Signal()
	self.mutex.Unlock()
	log.Println("Closed connection from", self.conn.RemoteAddr())
}

func (self *connection) push(data []byte) {
	self.mutex.Lock()
	self.store = append(self.store, data)
	self.cond.Signal()
	self.mutex.Unlock()
}
