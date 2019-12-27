package opentick

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/patrickmn/go-cache"
	"gopkg.in/mgo.v2/bson"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var defaultDBs []fdb.Transactor

var sNumDatabaseConn = 1
var sMaxConcurrency = 100
var sTimeout = 0
var activeConns int32
var respCache *cache.Cache
var sPermissionControl bool

func getDB() fdb.Transactor {
	return defaultDBs[rand.Intn(sNumDatabaseConn)]
}

func StartServer(addr string, fdbClusterFile string, numDatabaseConn, maxConcurrency, timeout int, cacheExpiration float64, permission bool) error {
	sPermissionControl = permission
	log.Print("Permission control: ", permission)
	if cacheExpiration > 0 {
		log.Println("cache enabled with expiration:", cacheExpiration, "seconds")
		respCache = cache.New(time.Duration(1000*cacheExpiration)*time.Millisecond, time.Duration(1000*cacheExpiration)*time.Millisecond)
	}
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
	if timeout > 0 {
		sTimeout = timeout
	}
	log.Println("timeout:", sTimeout, "(s)")
	defaultDBs = make([]fdb.Transactor, sNumDatabaseConn)
	for i := 0; i < sNumDatabaseConn; i++ {
		if fdbClusterFile == "" {
			defaultDBs[i] = fdb.MustOpenDefault()
		} else {
			// In the current release of fdb, the database name must be []byte("DB").
			defaultDBs[i] = fdb.MustOpen(fdbClusterFile, []byte("DB"))
		}
	}
	LoadUsers(getDB())
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
	user   *User
}

func (self *connection) Send(msg []byte) {
	self.ch <- msg
}

func handleConnection(conn net.Conn) {
	timeout := time.Duration(sTimeout) * time.Second
	atomic.AddInt32(&activeConns, 1)
	log.Println("New connection from", conn.RemoteAddr(), ", active:", activeConns)
	ch := make(chan []byte)
	user := &User{}
	user.isAdmin = !sPermissionControl
	if !user.isAdmin {
		fromLocal := strings.Contains(conn.RemoteAddr().String(), "127.0.0.1:")
		user.isAdmin = fromLocal
	}
	client := connection{ch: ch, conn: conn, user: user}
	client.cond = sync.NewCond(&client.mutex)
	defer client.close()
	go client.writeToConnection()
	go client.process()
	waitHeartbeat := false
	for {
		var head [4]byte
		tmp := head[:4]
		n := len(tmp)
		for n > 0 {
			if timeout > 0 {
				conn.SetReadDeadline(time.Now().Add(timeout))
			}
			n2, err := conn.Read(tmp)
			if err != nil {
				if e, ok := err.(net.Error); ok && e.Timeout() {
					if !waitHeartbeat {
						var size [4]byte
						binary.LittleEndian.PutUint32(size[:], 1)
						ch <- append(size[:], byte('H'))
						waitHeartbeat = true
						continue
					}
				}
				log.Println(err.Error(), "of connection", conn.RemoteAddr())
				return
			}
			waitHeartbeat = false
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
			if timeout > 0 {
				conn.SetReadDeadline(time.Now().Add(timeout))
			}
			n2, err := conn.Read(tmp)
			if err != nil {
				if e, ok := err.(net.Error); ok && e.Timeout() {
					if !waitHeartbeat {
						var size [4]byte
						binary.LittleEndian.PutUint32(size[:], 1)
						ch <- append(size[:], byte('H'))
						waitHeartbeat = true
						continue
					}
				}
				log.Println(err.Error(), "of connection", conn.RemoteAddr())
				return
			}
			waitHeartbeat = false
			tmp = tmp[n2:]
			n -= n2
		}
		client.push(body)
	}
}

func reply(cacheKey string, ticket int, res interface{}, ch chan []byte, useJson bool) {
	defer func() {
		if err := recover(); err != nil {
			// send on closed channel
		}
	}()
	var data []byte
	var err error
	key := "1"
	if _, ok := res.([]byte); ok {
		key = "2"
	}
	if useJson {
		data, err = json.Marshal(map[string]interface{}{"0": ticket, key: res})
	} else {
		data, err = bson.Marshal(map[string]interface{}{"0": ticket, key: res})
	}
	if err != nil {
		reply("", ticket, "Internal error: "+err.Error(), ch, useJson)
		return
	}
	if len(data) > math.MaxUint32 {
		reply("", ticket, "Results too large", ch, useJson)
		return
	}
	if cacheKey != "" {
		respCache.SetDefault(cacheKey, data)
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
	var prepared [][2]interface{}
	var usedDbName string
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
		user := self.user
		dbName := usedDbName
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
			var ticket int
			var cacheKey string
			var cmd string
			var sql string
			var preparedId int
			var ast *Ast
			var res interface{}
			var args []interface{}
			var toks []string
			var exists bool
			var stmt interface{}
			var cachedSql string
			var useCache int
			var schema *TableSchema
			var schema_res [2][]interface{}
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
				if string(body) == "H" { // heartbeat request
					self.ch <- []byte{0, 0, 0, 0}
					return
				}
				res = "Invalid bson: " + err.Error()
				goto reply
			}
			ticket, ok = data["0"].(int)
			if !ok {
				res = fmt.Sprint("Invalid ticket, expected int, got ", data["0"])
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
				self.mutex.Lock()
				if preparedId >= len(prepared) {
					self.mutex.Unlock()
					res = fmt.Sprint("Invalid preparedId ", preparedId)
					goto reply
				}
				stmt = prepared[preparedId][0]
				cachedSql = prepared[preparedId][1].(string)
				self.mutex.Unlock()
			} else if sql == "" {
				res = "Empty sql"
				goto reply
			}
			useCache, _ = data["4"].(int)
			if cmd == "run" {
				if stmt == nil {
					res, err = Execute(getDB(), dbName, sql, args, user)
				} else {
					if respCache != nil && useCache > 0 {
						if _, ok2 := stmt.(selectStmt); ok2 {
							cacheKey = cachedSql + " " + fmt.Sprint(args) + " " + fmt.Sprint(useJson)
							if cached, ok3 := respCache.Get(cacheKey); ok3 {
								res = cached
								cacheKey = ""
								goto reply
							}
						}
					}
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
				res, err = Resolve(getDB(), dbName, ast, user)
				if err != nil {
					res = err.Error()
					goto reply
				}
				self.mutex.Lock()
				prepared = append(prepared, [2]interface{}{res, sql})
				res = len(prepared) - 1
				self.mutex.Unlock()
			} else if cmd == "login" || cmd == "use" {
				if cmd == "login" {
					toks = strings.Split(sql, " ")
					if len(toks) < 2 || toks[0] == "" || toks[1] == "" {
						res = "Both username and password required"
						goto reply
					}
					res, _ = userMap.Load(toks[0])
					if res == nil {
						res = "Unknown username"
						goto reply
					}
					if !res.(*User).CheckPassword(toks[1]) {
						res = "Password mismatch"
						goto reply
					}
					self.mutex.Lock()
					self.user = res.(*User)
					user = self.user
					self.mutex.Unlock()
					if len(toks) == 2 {
						res = nil
						goto reply
					}
					sql = toks[2]
				}
				self.mutex.Lock()
				usedDbName = sql
				dbName = usedDbName
				self.mutex.Unlock()
				exists, err = HasDatabase(getDB(), dbName)
				if err != nil {
					res = err.Error()
					goto reply
				}
				if !exists {
					res = dbName + " does not exist"
				}
				if GetPerm(dbName, "", user) == NoPerm {
					res = "No permission"
				}
			} else if cmd == "meta" { // retrieve metadata
				toks = strings.Split(sql, " ")
				if len(toks) == 0 {
					res = "Please specify meta command"
					goto reply
				}
				switch toks[0] {
				case "list_databases":
					res, err = ListDatabases(getDB())
					if err != nil {
						res = err.Error()
					}
				case "list_tables":
					if dbName == "" {
						res = "Please select database first"
					} else {
						res, err = ListTables(getDB(), dbName)
						if err != nil {
							res = err.Error()
						}
					}
				case "schema":
					if len(toks) < 2 {
						res = "Please specify table name"
						goto reply
					}
					schema, err = GetTableSchema(getDB(), dbName, toks[1])
					if err != nil {
						res = err.Error()
						goto reply
					}
					for _, f := range schema.Keys {
						schema_res[0] = append(schema_res[0], []string{f.Name, f.Type.Name()})
					}
					for _, f := range schema.Values {
						schema_res[1] = append(schema_res[1], []string{f.Name, f.Type.Name()})
					}
					res = schema_res
				case "chgpasswd":
					if len(toks) < 2 {
						res = "Please specify new password"
						goto reply
					}
					if user.name == "" {
						res = "Not logged in"
						goto reply
					}
					err = user.UpdatePasswd(getDB(), toks[1])
					if err != nil {
						res = err.Error()
					}
				case "reload_users":
					if !user.isAdmin {
						res = "No permission"
					} else {
						LoadUsers(getDB())
					}
				default:
					res = "Invalid meta command"
				}
			} else {
				res = "Invalid command " + cmd
			}
		reply:
			reply(cacheKey, ticket, res, self.ch, useJson)
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
	atomic.AddInt32(&activeConns, -1)
	log.Println("Closed connection from", self.conn.RemoteAddr(), ", active:", activeConns)
}

func (self *connection) push(data []byte) {
	self.mutex.Lock()
	self.store = append(self.store, data)
	self.cond.Signal()
	self.mutex.Unlock()
}
