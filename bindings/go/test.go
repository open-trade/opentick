package main

import (
	"fmt"
	"github.com/opentradesolutions/opentick/client"
	"log"
	"time"
)

func main() {
	conn, err0 := client.Connect("127.0.0.1", 1116, "")
	log.Println("connected")
	assertEqual(nil, err0)
	res, err := conn.Execute("create database if not exists test")
	assertEqual(nil, err)
	conn.Use("test")
	res, err = conn.Execute("create table if not exists test(sec int, interval int, tm timestamp, open double, high double, low double, close double, v double, vwap double, primary key(sec, interval, tm))")
	assertEqual(nil, err)
	res, err = conn.Execute("delete from test where sec=?", 1)
	assertEqual(nil, err)
	log.Println("records deleted")
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
				assertEqual(nil, err)
				futs = append(futs, fut)
			}
		}
		now2 := time.Now()
		log.Println(now2.Sub(now), "async done")
		for _, f := range futs {
			f.Get()
		}
		now3 := time.Now()
		log.Println(now3.Sub(now2), now3.Sub(now), i, len(futs), "all insert futures get done")
		res, err = futs[0].Get(1)
		assertEqual("Timeout", err.Error())
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
			assertEqual(nil, err)
			futs = append(futs, fut)
		}
		now2 = time.Now()
		log.Println(now2.Sub(now), "async done")
		for _, f := range futs {
			f.Get()
		}
		now3 = time.Now()
		log.Println(now3.Sub(now2), now3.Sub(now), i, len(futs), "all batch insert futures get done")
		futs = nil
		for j := 0; j <= i; j++ {
			fut, err := conn.ExecuteAsync("select * from test where sec=1 and interval=? and tm>=? and tm<=?", j, client.SplitRange(tm, tm2, 10))
			assertEqual(nil, err)
			futs = append(futs, fut)
		}
		res = nil
		for _, f := range futs {
			tmp, err := f.Get()
			assertEqual(nil, err)
			res = append(res, tmp...)
		}
		now4 := time.Now()
		log.Println(now4.Sub(now3), len(res), "retrieved with ranges")
		assertEqual((i+1)*n1*n2, len(res))
		assertEqual(tm.UTC(), res[0][2])
		assertEqual(tm2.UTC(), res[len(res)-1][2])
		res, err = conn.Execute("select tm from test where sec=1 and interval=? and tm=?", i, tm)
		assertEqual(nil, err)
		assertEqual(tm.UTC(), res[0][0])
		res, err = conn.Execute("select tm from test where sec=1 and interval=? limit -2", i)
		assertEqual(nil, err)
		assertEqual(2, len(res))
		assertEqual(tm2.UTC(), res[0][0])
		futs = nil
		for j := 0; j <= i; j++ {
			fut, err := conn.ExecuteAsync("select * from test where sec=1 and interval=?", j)
			assertEqual(nil, err)
			futs = append(futs, fut)
		}
		res = nil
		for _, f := range futs {
			tmp, err := f.Get()
			assertEqual(nil, err)
			res = append(res, tmp...)
		}
		now5 := time.Now()
		log.Println(now5.Sub(now4), len(res), "retrieved with async")
		assertEqual((i+1)*n1*n2, len(res))
		assertEqual(tm.UTC(), res[0][2])
		assertEqual(tm2.UTC(), res[len(res)-1][2])
		if i < 20 {
			res, err = conn.Execute("select * from test where sec=1")
			assertEqual(nil, err)
			now6 := time.Now()
			log.Println(now6.Sub(now5), len(res), "retrieved with one sync")
			assertEqual((i+1)*n1*n2, len(res))
			assertEqual(tm.UTC(), res[0][2])
			assertEqual(tm2.UTC(), res[len(res)-1][2])
		}
		log.Println()
	}
}

func assertEqual(a interface{}, b interface{}) {
	if a != b {
		panic(fmt.Sprintln("\nExpected:", a, "\nGot:", b))
	}
}
