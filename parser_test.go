package opentick

import (
	"github.com/alecthomas/repr"
	"testing"
)

// var sqlStmt = "INSERT into x(x, y) values(1, 2)"

var sqlStmt = "create table a.b(x int, y double, primary key (x, y))"

// var sqlStmt = "select a, b from test where a > 1.2 and (b < 2 - 1 or b in (1,2)) limit -2"

func Test_Parse(t *testing.T) {
	expr, err := Parse(sqlStmt)
	repr.Println(expr, repr.Indent("  "), repr.OmitEmpty(true))
	if err != nil {
		t.Error(err)
	}
}

func Benchmark_Parse(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ { //use b.N for looping
		_, err := Parse(sqlStmt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Test_CreateTableSql(t *testing.T) {
	sqlCreateTable1 := `
	create table test.test(
		symbol_id bigint,
		interval int, 
  	tm timestamp,
		open double,
		high double,
		low double,
		close double,
		volume double,
		primary key (symbol_id, interval, tm)
	)
  `
	_, err := Parse(sqlCreateTable1)
	if err != nil {
		t.Error(err)
	}
}
