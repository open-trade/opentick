package opentick

import (
	"github.com/opentradesolutions/opentick/client"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func Test_Split(t *testing.T) {
	a := time.Date(1984, time.November, 3, 13, 0, 0, 0, time.UTC)
	b := time.Date(2018, time.November, 10, 18, 0, 0, 1, time.UTC)
	c := client.SplitRange(a, b, 10)
	a1 := a.Add(time.Duration(b.Sub(a).Nanoseconds() / int64(10)))
	assert.Equal(t, a, c[0][0])
	assert.Equal(t, a1, c[0][1])
	b0 := a.Add(time.Duration(b.Sub(a).Nanoseconds() / int64(10) * int64(9)))
	assert.Equal(t, b0, c[9][0])
	c2 := client.SplitRange(100, 1000, 11)
	assert.Equal(t, 100+(1000-100)/11*10, c2[10][0])
	assert.Equal(t, 100+(1000-100)/11, c2[0][1])
	assert.Equal(t, 100+(1000-100)/11*5, c2[5][0])
	assert.Equal(t, 100+(1000-100)/11*6, c2[5][1])
	c3 := client.SplitRange(100., 1000., 11)
	assert.Equal(t, 100.+(1000.-100.)/11*10, c3[10][0])
	assert.Equal(t, 100.+(1000.-100.)/11, c3[0][1])
	assert.Equal(t, 100.+(1000.-100.)/11*5, c3[5][0])
	assert.Equal(t, 100.+(1000.-100.)/11*6, c3[5][1])
}

func Test_Server(t *testing.T) {
	port, _ := freeport.GetFreePort()
	go StartServer(":"+strconv.FormatInt(int64(port), 10), "", 1, 0, 0)
	time.Sleep(100 * time.Millisecond)
	conn, err := client.Connect("", port, "")
	assert.Equal(t, nil, err)
	var res [][]interface{}
	_, err = conn.Execute("create database if not exists test")
	assert.Equal(t, nil, err)
	conn.Close()
	conn, err = client.Connect("", port, "test")
	defer conn.Close()
	assert.Equal(t, nil, err)
	_, err = conn.Execute("create table if not exists test(a int, primary key(a))")
	assert.Equal(t, nil, err)
	_, err = conn.Execute("drop table test")
	assert.Equal(t, nil, err)
	res, err = conn.Execute("select * from test where a=1")
	assert.Equal(t, "Table test.test does not exists", err.Error())
	assert.Equal(t, [][]interface{}(nil), res)
	_, err = conn.Execute("create table test(sec int, interval int, tm timestamp, open double, high double, low double, close double, v double,vwap double, primary key(sec, interval, tm))")
	assert.Equal(t, nil, err)
	tm := time.Now()
	_, err = conn.Execute("insert into test(sec, interval, tm, open) values(?, ?, ?, ?)", 1, 2, tm, 2.2)
	_, err = conn.Execute("insert into test(sec, interval, tm, open) values(?, ?, ?, ?)", 1, 2, tm.Add(time.Second), 4)
	assert.Equal(t, nil, err)
	res, err = conn.Execute("select * from test where sec=? and interval=? and tm>=? and tm<=?", 1, 2, client.SplitRange(tm, tm.Add(time.Second), 10))
	assert.Equal(t, 2, len(res))
	assert.Equal(t, tm.UTC(), res[0][2])
	res, err = conn.Execute("select * from test where sec=? and interval=?", 1, 2)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(res))
	assert.Equal(t, tm.UTC(), res[0][2])
	res, err = conn.Execute("select * from test where sec=? and interval=? and tm=?", 1, 2, tm)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, tm.UTC(), res[0][2])
	res, err = conn.Execute("select * from test where sec=? and interval=? and tm<?", 1, 2, tm.Add(time.Second))
	assert.Equal(t, tm.UTC(), res[0][2])
	res, err = conn.Execute("delete from test where sec=? and interval=? and tm=?", 1, 2, tm)
	assert.Equal(t, nil, err)
	res, err = conn.Execute("select * from test where sec=? and interval=? and tm=?", 1, 2, tm)
	assert.Equal(t, 0, len(res))
	res, err = conn.Execute("select * from test where sec=? and interval=?", 1, 2)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, float64(4), res[0][3])
	argsArray := [][]interface{}{[]interface{}{tm, 5}, []interface{}{2}}
	err = conn.BatchInsert("insert into test(sec, interval, tm, open) values(?, ?, ?, ?)", argsArray)
	assert.Equal(t, "All array must the same size", err.Error())
	argsArray = [][]interface{}{[]interface{}{tm, 5}, []interface{}{2., 3}}
	err = conn.BatchInsert("insert into test(sec, interval, tm, open) values(?, ?, ?, ?)", argsArray)
	assert.Equal(t, "Expected 4 arguments, got 2", err.Error())
	argsArray = [][]interface{}{[]interface{}{tm, 5}, []interface{}{2., 3}}
	err = conn.BatchInsert("insert into test(sec, interval, tm, open) values(1, 2, ?, ?)", argsArray)
	assert.Equal(t, "Invalid float64 value (2) for \"tm\" of Timestamp", err.Error())
	res, err = conn.Execute("select open from test where sec=? and interval=? and tm=?", 1, 2, tm)
	assert.Equal(t, 0, len(res))
	_, err = conn.Execute("alter table test rename tm to time")
	assert.Equal(t, nil, err)
	// schema is cached, so tm still work even it renamed
	res, err = conn.Execute("select open from test where sec=? and interval=? and tm=?", 1, 2, tm)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(res))
	argsArray = [][]interface{}{[]interface{}{tm, 5}, []interface{}{tm, 3}}
	err = conn.BatchInsert("insert into test(sec, interval, tm, open) values(1, 2, ?, ?)", argsArray)
	res, err = conn.Execute("select open from test where sec=? and interval=? and tm=?", 1, 2, tm)
	assert.Equal(t, float64(3), res[0][0])
	res, err = conn.Execute("select open from test where sec=? and interval=? and time=?", 1, 2, tm)
	assert.Equal(t, nil, err)
	assert.Equal(t, float64(3), res[0][0])
	conn.Execute("drop table test")
}

func Benchmark_client_insert_sync(b *testing.B) {
	port, _ := freeport.GetFreePort()
	go StartServer(":"+strconv.FormatInt(int64(port), 10), "", 1, 0, 0)
	time.Sleep(100 * time.Millisecond)
	conn, err := client.Connect("", port, "test")
	_, err = conn.Execute("create database if not exists test")
	_, err = conn.Execute("create table test(sec int, interval int, tm timestamp, open double, high double, low double, close double, v double,vwap double, primary key(sec, interval, tm))")
	tm := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm = tm.Add(time.Second)
		_, err = conn.Execute("insert into test(sec, interval, tm, open) values(?, ?, ?, ?)", 1, 2, tm, i)
		if err != nil {
			b.Fatal(err)
		}
	}
	conn.Execute("drop table test")
}

func Benchmark_insert_not_prepared(b *testing.B) {
	port, _ := freeport.GetFreePort()
	go StartServer(":"+strconv.FormatInt(int64(port), 10), "", 1, 0, 0)
	time.Sleep(100 * time.Millisecond)
	conn, err := client.Connect("", port, "test")
	_, err = conn.Execute("create database if not exists test")
	_, err = conn.Execute("create table test(sec int, interval int, tm timestamp, open double, high double, low double, close double, v double,vwap double, primary key(sec, interval, tm))")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = Execute(getDB(), "", "insert into test.test(sec, interval, tm, open) values(?, ?, ?, ?)", []interface{}{1, 2, i, i})
		if err != nil {
			b.Fatal(err)
		}
	}
	conn.Execute("drop table test")
}

func Benchmark_insert_prepared(b *testing.B) {
	port, _ := freeport.GetFreePort()
	go StartServer(":"+strconv.FormatInt(int64(port), 10), "", 1, 0, 0)
	time.Sleep(100 * time.Millisecond)
	conn, err := client.Connect("", port, "test")
	_, err = conn.Execute("create database if not exists test")
	_, err = conn.Execute("create table test(sec int, interval int, tm timestamp, open double, high double, low double, close double, v double,vwap double, primary key(sec, interval, tm))")
	ast, _ := Parse("insert into test.test(sec, interval, tm, open) values(?, ?, ?, ?)")
	stmt, _ := Resolve(getDB(), "", ast)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = ExecuteStmt(getDB(), stmt, []interface{}{1, 2, i, i})
		if err != nil {
			b.Fatal(err)
		}
	}
	conn.Execute("drop table test")
}
