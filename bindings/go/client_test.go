package main

import (
	"github.com/opentradesolutions/opentick/client"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func Test_Client(t *testing.T) {
	conn, err0 := client.Connect("", 1116, "")
	assert.Equal(t, nil, err0)
	res, err := conn.Execute("create database if not exists test")
	assert.Equal(t, nil, err)
	conn.Use("test")
	res, err = conn.Execute("create table if not exists test(sec int, interval int, tm timestamp, open double, high double, low double, close double, v double, vwap double, primary key(sec, interval, tm))")
	assert.Equal(t, nil, err)
	res, err = conn.Execute("delete from test where sec=?", 1)
	assert.Equal(t, nil, err)
	tm := time.Now()
	for i := 0; i < 100; i++ {
		n1 := 10
		n2 := 10000
		tm2 := tm
		var futs []client.Future
		now := time.Now()
		for j := 0; j < n1; j++ {
			for k := 0; k < n2; k++ {
				ms := j*n2 + k
				tm2 = tm.Add(time.Duration(ms) * time.Microsecond)
				fut, err := conn.ExecuteAsync(
					"insert into test(sec, interval, tm, open, high, low, close, v, vwap) values(?, ?, ?, ?, ?, ?, ?, ?, ?)",
					1, i, tm2, 2.2, 2.4, 2.1, 2.3, 1000000, 2.25)
				assert.Equal(t, nil, err)
				futs = append(futs, fut)
			}
		}
		now2 := time.Now()
		log.Println(now2.Sub(now), "async done")
		for _, f := range futs {
			f.Get()
		}
		now3 := time.Now()
		log.Println(now3.Sub(now2), i, len(futs), "all insert futures Get done")
		// futs[0].Get()
		futs = nil
		now = time.Now()
		for j := 0; j < n1; j++ {
			var args_array [][]interface{}
			for k := 0; k < n2; k++ {
				ms := j*n2 + k
				tm2 = tm.Add(time.Duration(ms) * time.Microsecond)
				args_array = append(args_array, []interface{}{1, i, tm2, 2.2, 2.4, 2.1, 2.3, 1000000, 2.25})
			}
			// the batch size is limited by foundationdb transaction size
			// https://apple.github.io/foundationdb/known-limitations.html
			fut, err := conn.BatchInsertAsync(
				"insert into test(sec, interval, tm, open, high, low, close, v, vwap) values(?, ?, ?, ?, ?, ?, ?, ?, ?)",
				args_array)
			assert.Equal(t, nil, err)
			futs = append(futs, fut)
		}
		now2 = time.Now()
		log.Println(now2.Sub(now), "async done")
		for _, f := range futs {
			f.Get()
		}
		now3 = time.Now()
		log.Println(now3.Sub(now2), i, len(futs), "all batch insert futures Get done")
		for j := 0; j < i+1; j++ {
			tmp, err := conn.Execute("select * from test where sec=1 and interval=? and tm>=? and tm<=?", j,
				client.SplitRange(tm, tm2, 10))
			assert.Equal(t, nil, err)
			res = append(res, tmp...)
		}
		now4 := time.Now()
		log.Println(now4.Sub(now3), len(res), "retrieved with ranges")
		assert.Equal(t, (i+1)*n1*n2, len(res))
		assert.Equal(t, tm, res[0][2])
		assert.Equal(t, tm2, res[len(res)-1][2])
		res, err = conn.Execute("select tm from test where sec=1 and interval=? and tm=?", i, tm)
		assert.Equal(t, nil, err)
		assert.Equal(t, tm, res[0][0])
		res, err = conn.Execute("select tm from test where sec=1 and interval=? limit -2", i)
		assert.Equal(t, nil, err)
		assert.Equal(t, 2, len(res))
		assert.Equal(t, tm2, res[0][0])
		futs = nil
		for j := 0; j < i+1; j++ {
			fut, err := conn.ExecuteAsync("select * from test where sec=1 and interval=?", j)
			assert.Equal(t, nil, err)
			futs = append(futs, fut)
		}
		now5 := time.Now()
		log.Println(now5.Sub(now4), "async done")
		res = nil
		for _, f := range futs {
			tmp, err := f.Get()
			assert.Equal(t, nil, err)
			res = append(res, tmp...)
		}
		now6 := time.Now()
		log.Println(now6.Sub(now4), len(res), "retrieved with async")
		assert.Equal(t, (i+1)*n1*n2, len(res))
		assert.Equal(t, tm, res[0][2])
		assert.Equal(t, tm2, res[len(res)-1][2])
		res, err = conn.Execute("select * from test where sec=1")
		assert.Equal(t, nil, err)
		now7 := time.Now()
		log.Println(now7.Sub(now4), len(res), "retrieved with one sync")
		assert.Equal(t, (i+1)*n1*n2, len(res))
		assert.Equal(t, tm, res[0][2])
		assert.Equal(t, tm2, res[len(res)-1][2])
		log.Println()
	}
}
