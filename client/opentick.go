package client

import (
	"encoding/binary"
	"errors"
	"gopkg.in/mgo.v2/bson"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Future interface {
	Get() ([][]interface{}, error)
}

type Connection interface {
	Execute(sql string, args ...interface{}) (ret [][]interface{}, err error)
	ExecuteAsync(sql string, args ...interface{}) (Future, error)
	Close()
}

func Connect(host string, port int, dbName string) (ret Connection, err error) {
	conn, err := net.Dial("tcp", host+":"+strconv.FormatInt(int64(port), 10))
	if err != nil {
		return
	}
	m := &sync.Mutex{}
	c := &connection{
		conn:      conn,
		store:     make(map[int]interface{}),
		mutexCond: m,
		cond:      sync.NewCond(m),
	}
	go recv(c)
	ticker := c.getTicker()
	if dbName != "" {
		cmd := map[string]interface{}{"0": ticker, "1": "use", "2": dbName}
		err = c.send(cmd)
		if err != nil {
			c.Close()
			return
		}
		f := future{ticker, c}
		_, err = f.get()
		if err != nil {
			c.Close()
			return
		}
	}
	ret = c
	return
}

type future struct {
	ticker int
	conn   *connection
}

func (self *future) getStore(ticker int) (ret interface{}, ok bool) {
	self.conn.mutex.Lock()
	defer self.conn.mutex.Unlock()
	ret, ok = self.conn.store[ticker]
	if ok && ticker != -1 {
		delete(self.conn.store, ticker)
	}
	return
}

func (self *future) get() (interface{}, error) {
	for {
		if tmp, ok := self.getStore(self.ticker); ok {
			data := tmp.(map[string]interface{})
			res, _ := data["1"]
			if str, ok := res.(string); ok {
				return nil, errors.New(str)
			}
			return res, nil
		} else if tmp, ok := self.getStore(-1); ok {
			return nil, tmp.(error)
		}
		self.conn.mutexCond.Lock()
		self.conn.cond.Wait()
		self.conn.mutexCond.Unlock()
	}
}

func (self *future) Get() (ret [][]interface{}, err error) {
	var res interface{}
	res, err = self.get()
	if res == nil || err != nil {
		return
	}
	if res2, ok := res.([]interface{}); ok {
		for _, rec := range res2 {
			if rec2, ok2 := rec.([]interface{}); ok2 {
				for i, v := range rec2 {
					if v2, ok := v.([]interface{}); ok {
						if len(v2) == 2 {
							if sec, ok1 := v2[0].(int64); ok1 {
								if nsec, ok2 := v2[1].(int64); ok2 {
									rec2[i] = time.Unix(sec, nsec).UTC()
								}
							}
						}
					}
				}
				ret = append(ret, rec2)
			}
		}
	}
	return
}

type connection struct {
	conn          net.Conn
	tickerCounter int64
	prepared      sync.Map
	store         map[int]interface{}
	mutex         sync.Mutex
	cond          *sync.Cond
	mutexCond     *sync.Mutex
}

func (self *connection) Close() {
	self.conn.Close()
}

func (self *connection) Execute(sql string, args ...interface{}) (ret [][]interface{}, err error) {
	var fut Future
	fut, err = self.ExecuteAsync(sql, args...)
	if err != nil {
		return
	}
	return fut.Get()
}

func (self *connection) ExecuteAsync(sql string, args ...interface{}) (ret Future, err error) {
	prepared := -1
	var cmd map[string]interface{}
	if len(args) > 0 {
		for i, v := range args {
			if v2, ok := v.(time.Time); ok {
				args[i] = []int64{v2.Unix(), int64(v2.Nanosecond())}
			}
		}
		if tmp, ok := self.prepared.Load(sql); ok {
			prepared = tmp.(int)
		} else {
			ticker := self.getTicker()
			cmd = map[string]interface{}{"0": ticker, "1": "prepare", "2": sql}
			err = self.send(cmd)
			if err != nil {
				return
			}
			f := future{ticker, self}
			res, err2 := f.get()
			if err2 != nil {
				err = err2
				return
			}
			prepared = res.(int)
			self.prepared.Store(sql, prepared)
		}
	}
	ticker := self.getTicker()
	cmd = map[string]interface{}{"0": ticker, "1": "run", "2": sql, "3": args}
	if prepared >= 0 {
		cmd["2"] = prepared
	}
	err = self.send(cmd)
	if err != nil {
		return
	}
	ret = &future{ticker, self}
	return
}

func (self *connection) getTicker() int {
	return int(atomic.AddInt64(&self.tickerCounter, 1))
}

func (self *connection) send(data map[string]interface{}) error {
	out, err := bson.Marshal(data)
	if err != nil {
		panic(err)
	}
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(out)))
	out = append(size[:], out...)
	n := len(out)
	self.mutex.Lock()
	defer self.mutex.Unlock()
	for n > 0 {
		n2, err := self.conn.Write(out)
		if err != nil {
			return err
		}
		out = out[n2:]
		n -= n2
	}
	return nil
}

func (self *connection) notify(ticker int, msg interface{}) {
	self.mutex.Lock()
	self.store[ticker] = msg
	self.cond.Broadcast()
	self.mutex.Unlock()
}

func recv(c *connection) {
	defer c.cond.Broadcast()
	for {
		var head [4]byte
		tmp := head[:4]
		n := len(tmp)
		for n > 0 {
			n2, err := c.conn.Read(tmp)
			if err != nil {
				c.notify(-1, err)
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
			n2, err := c.conn.Read(tmp)
			if err != nil {
				c.notify(-1, err)
				return
			}
			tmp = tmp[n2:]
			n -= n2
		}
		var data map[string]interface{}
		var err error
		err = bson.Unmarshal(body, &data)
		if err != nil {
			c.notify(-1, err)
			return
		}
		c.notify(data["0"].(int), data)
	}
}
