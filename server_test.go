package opentick

import (
	"github.com/opentradesolutions/opentick/client"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_Server(t *testing.T) {
	port, _ := freeport.GetFreePort()
	go StartServer("", port)
	time.Sleep(100 * time.Millisecond)
	conn, err := client.Connect("", port, "")
	assert.Equal(t, nil, err)
	var ret [][]interface{}
	_, err = conn.Execute("create database if not exists test")
	assert.Equal(t, nil, err)
	conn.Close()
	conn, err = client.Connect("", port, "test")
	assert.Equal(t, nil, err)
	_, err = conn.Execute("create table if not exists test(a int, primary key(a))")
	assert.Equal(t, nil, err)
	_, err = conn.Execute("drop table test")
	assert.Equal(t, nil, err)
	ret, err = conn.Execute("select * from test where a=1")
	assert.Equal(t, "Table test.test does not exists", err.Error())
	assert.Equal(t, [][]interface{}(nil), ret)
	_, err = conn.Execute("create table test(sec int, interval int, tm timestamp, open double, high double, low double, close double, v double,vwap double, primary key(sec, interval, tm))")
	assert.Equal(t, nil, err)
	_, err = conn.Execute("insert into test(sec, interval, tm, open) values(?, ?, ?, ?)", []interface{}{1, 2, "x", 2.2})
	assert.Equal(t, nil, err)
	defer conn.Close()
}
