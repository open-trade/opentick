package client

import (
	"encoding/binary"
	"errors"
	"gopkg.in/mgo.v2/bson"
	"net"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Future interface {
	Get(timeout ...int) ([][]interface{}, error) // timeout in seconds
}

type Connection interface {
	Use(dbName string) (err error)
	Execute(sql string, args ...interface{}) (ret [][]interface{}, err error)
	ExecuteAsync(sql string, args ...interface{}) (Future, error)
	BatchInsert(sql string, argsArray [][]interface{}) (err error)
	BatchInsertAsync(sql string, argsArray [][]interface{}) (Future, error)
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
	if dbName != "" {
		err = c.Use(dbName)
		if err != nil {
			return
		}
	}
	ret = c
	return
}

type RangeArray [][2]interface{}

func SplitRange(start interface{}, end interface{}, numParts int) (parts RangeArray) {
	if reflect.TypeOf(start) != reflect.TypeOf(end) || numParts <= 1 {
		return
	}

	switch start.(type) {
	case int:
		a := start.(int)
		b := end.(int)
		d := (b - a) / numParts
		for i := 0; i < numParts; i++ {
			tmp := a + i*d
			parts = append(parts, [2]interface{}{tmp, tmp + d})
		}
		parts[numParts-1][1] = b
	case int64:
		a := start.(int64)
		b := end.(int64)
		d := (b - a) / int64(numParts)
		for i := 0; i < numParts; i++ {
			tmp := a + int64(i)*d
			parts = append(parts, [2]interface{}{tmp, tmp + d})
		}
		parts[numParts-1][1] = b
	case int32:
		a := start.(int32)
		b := end.(int32)
		d := (b - a) / int32(numParts)
		for i := 0; i < numParts; i++ {
			tmp := a + int32(i)*d
			parts = append(parts, [2]interface{}{tmp, tmp + d})
		}
		parts[numParts-1][1] = b
	case float64:
		a := start.(float64)
		b := end.(float64)
		d := (b - a) / float64(numParts)
		for i := 0; i < numParts; i++ {
			tmp := a + float64(i)*d
			parts = append(parts, [2]interface{}{tmp, tmp + d})
		}
		parts[numParts-1][1] = b
	case float32:
		a := start.(float32)
		b := end.(float32)
		d := (b - a) / float32(numParts)
		for i := 0; i < numParts; i++ {
			tmp := a + float32(i)*d
			parts = append(parts, [2]interface{}{tmp, tmp + d})
		}
		parts[numParts-1][1] = b
	case time.Time:
		a := start.(time.Time)
		b := end.(time.Time)
		d := time.Duration(b.Sub(a).Nanoseconds() / int64(numParts))
		tmp := a
		for i := 0; i < numParts; i++ {
			tmp1 := tmp.Add(d)
			parts = append(parts, [2]interface{}{tmp, tmp1})
			tmp = tmp1
		}
		parts[numParts-1][1] = b
	}
	return
}

type future struct {
	ticker int
	conn   *connection
}

func (self *future) get(timeout ...int) (interface{}, error) {
	self.conn.mutexCond.Lock()
	defer self.conn.mutexCond.Unlock()
	var timeout2 time.Duration
	if len(timeout) > 0 {
		timeout2 = time.Duration(time.Duration(timeout[0]) * time.Second)
	}
	tm := time.Now()
	for {
		if tmp, ok := self.conn.store[self.ticker]; ok {
			delete(self.conn.store, self.ticker)
			data := tmp.(map[string]interface{})
			res, _ := data["1"]
			if str, ok := res.(string); ok {
				return nil, errors.New(str)
			}
			return res, nil
		} else if tmp, ok := self.conn.store[-1]; ok && tmp != nil {
			return nil, tmp.(error)
		}
		self.conn.cond.Wait()
		if timeout2 > 0 && time.Now().Sub(tm) >= timeout2 {
			return nil, errors.New("Timeout")
		}
	}
}

func (self *future) Get(timeout ...int) (ret [][]interface{}, err error) {
	var res interface{}
	res, err = self.get(timeout...)
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

func (self *connection) Use(dbName string) (err error) {
	ticker := self.getTicker()
	cmd := map[string]interface{}{"0": ticker, "1": "use", "2": dbName}
	err = self.send(cmd)
	if err != nil {
		self.Close()
		return
	}
	f := future{ticker, self}
	_, err = f.get()
	if err != nil {
		self.Close()
		return
	}
	return
}

func (self *connection) Close() {
	self.conn.Close()
}

func (self *connection) BatchInsert(sql string, argsArray [][]interface{}) (err error) {
	var fut Future
	fut, err = self.BatchInsertAsync(sql, argsArray)
	if err != nil {
		return
	}
	_, err = fut.Get()
	return
}

func (self *connection) BatchInsertAsync(sql string, argsArray [][]interface{}) (fut Future, err error) {
	if len(argsArray) == 0 {
		err = errors.New("argsArray required")
		return
	}
	for _, args := range argsArray {
		convertTimestamp(args)
	}
	var prepared int
	prepared, err = self.prepare(sql)
	if err != nil {
		return
	}
	ticker := self.getTicker()
	cmd := map[string]interface{}{"0": ticker, "1": "batch", "2": prepared, "3": argsArray}
	err = self.send(cmd)
	if err != nil {
		return
	}
	fut = &future{ticker, self}
	return
}

func (self *connection) prepare(sql string) (prepared int, err error) {
	if tmp, ok := self.prepared.Load(sql); ok {
		prepared = tmp.(int)
	} else {
		ticker := self.getTicker()
		cmd := map[string]interface{}{"0": ticker, "1": "prepare", "2": sql}
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
	return
}

func convertTimestamp(args []interface{}) {
	for i, v := range args {
		if v2, ok := v.(time.Time); ok {
			args[i] = [2]int64{v2.Unix(), int64(v2.Nanosecond())}
		}
	}
}

func (self *connection) executeRanges(sql string, args ...interface{}) (ret [][]interface{}, err error) {
	n := len(args) - 1
	ranges := args[n].(RangeArray)
	var futs []Future
	for _, r := range ranges {
		args2 := append(args[:n], r[:]...)
		fut, err2 := self.ExecuteAsync(sql, args2...)
		if err2 != nil {
			err = err2
			return
		}
		futs = append(futs, fut)
	}
	for _, fut := range futs {
		ret2, err2 := fut.Get()
		if err2 != nil {
			err = err2
			return
		}
		if len(ret2) > 0 {
			if len(ret) > 0 && reflect.DeepEqual(ret[len(ret)-1], ret2[0]) {
				ret2 = ret2[1:]
			}
			ret = append(ret, ret2...)
		}
	}
	return
}

func (self *connection) Execute(sql string, args ...interface{}) (ret [][]interface{}, err error) {
	if len(args) > 0 {
		if _, ok := args[len(args)-1].(RangeArray); ok {
			return self.executeRanges(sql, args...)
		}
	}
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
		if _, ok := args[len(args)-1].(RangeArray); ok {
			err = errors.New("RangeArray not supported in ExecuteAsync, please use Execute instead")
			return
		}
		convertTimestamp(args)
		prepared, err = self.prepare(sql)
		if err != nil {
			return
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
	self.mutexCond.Lock()
	self.store[ticker] = msg
	self.cond.Broadcast()
	self.mutexCond.Unlock()
}

func recv(c *connection) {
	defer c.cond.Broadcast()
	timeout := 100 * time.Millisecond
	for {
		var head [4]byte
		tmp := head[:4]
		n := len(tmp)
		for n > 0 {
			c.conn.SetReadDeadline(time.Now().Add(timeout))
			n2, err := c.conn.Read(tmp)
			if err != nil {
				if e, ok := err.(net.Error); ok && e.Timeout() {
					c.cond.Broadcast()
					continue
				}
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
			c.conn.SetReadDeadline(time.Now().Add(timeout))
			n2, err := c.conn.Read(tmp)
			if err != nil {
				if e, ok := err.(net.Error); ok && e.Timeout() {
					c.cond.Broadcast()
					continue
				}
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
