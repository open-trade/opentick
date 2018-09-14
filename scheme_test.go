package opentick

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"testing"
)

var d = TableColDef{"Test", Double}

func Test_EncodeTableColDef(t *testing.T) {
	bytes := d.encode()
	d2 := TableColDef{}
	decodeTableColDef(bytes, &d2, schemeVersion)
	if d2.Name != d.Name || d2.Type != d.Type {
		t.Error("failed")
	}
}

func Benchmark_DecodeTableColDef(b *testing.B) {
	b.StopTimer()
	bytes := d.encode()
	b.StartTimer()
	for i := 0; i < b.N; i++ { //use b.N for looping
		d2 := TableColDef{}
		decodeTableColDef(bytes, &d2, schemeVersion)
	}
}

var tbl = TableScheme{[]TableColDef{d, d, d}, []uint32{0, 1, 2}}

func Test_EncodeTableScheme(t *testing.T) {
	bytes := tbl.encode()
	t2 := decodeTableScheme(bytes)
	if t2.Cols[2] != tbl.Cols[2] || t2.Key[2] != tbl.Key[2] {
		t.Error("failed")
	}
}

func Benchmark_DecodeTableScheme(b *testing.B) {
	b.StopTimer()
	bytes := tbl.encode()
	b.StartTimer()
	for i := 0; i < b.N; i++ { //use b.N for looping
		decodeTableScheme(bytes)
	}
}

func Test_CreateTable(t *testing.T) {
	fdb.MustAPIVersion(FdbVersion)
	var db = fdb.MustOpenDefault()
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
	ast, _ := Parse(sqlCreateTable1)
	err := CreateTable(db, "", ast.Create.Table)
	if err != nil {
		t.Error(err)
	}
}
