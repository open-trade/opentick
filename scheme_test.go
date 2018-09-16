package opentick

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/stretchr/testify/assert"
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

func Benchmark_SubspacePack(b *testing.B) {
	b.StopTimer()
	sub := subspace.FromBytes([]byte("test"))
	t := tuple.Tuple{"test", "test", "test"}
	b.StartTimer()
	for i := 0; i < b.N; i++ { //use b.N for looping
		sub.Pack(t)
	}
}

func Benchmark_SubspaceUnpack(b *testing.B) {
	b.StopTimer()
	sub := subspace.FromBytes([]byte("test"))
	t := tuple.Tuple{"test", "test", "test"}
	p := sub.Pack(t)
	b.StartTimer()
	for i := 0; i < b.N; i++ { //use b.N for looping
		sub.Unpack(p)
	}
}

func Benchmark_TuplePack(b *testing.B) {
	b.StopTimer()
	t := tuple.Tuple{"test", "test", "test"}
	b.StartTimer()
	for i := 0; i < b.N; i++ { //use b.N for looping
		t.Pack()
	}
}

func Benchmark_TupleUnpack(b *testing.B) {
	b.StopTimer()
	t := tuple.Tuple{"test", "test", "test"}
	p := t.Pack()
	b.StartTimer()
	for i := 0; i < b.N; i++ { //use b.N for looping
		tuple.Unpack(p)
	}
}

func concat(a []byte, b ...byte) []byte {
	r := make([]byte, len(a)+len(b))
	copy(r, a)
	copy(r[len(a):], b)
	return r
}

func Benchmark_concat(b *testing.B) {
	b.StopTimer()
	x := []byte("test")
	y := []byte("test")
	b.StartTimer()
	for i := 0; i < b.N; i++ { //use b.N for looping
		concat(x, y...)
	}
}

func Benchmark_append(b *testing.B) {
	b.StopTimer()
	x := []byte("test")
	y := []byte("test")
	var z []byte
	b.StartTimer()
	for i := 0; i < b.N; i++ { //use b.N for looping
		z = append(x, y...)
	}
	fmt.Print(len(z))
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
	DropDatabase(db, "test")
	ast, _ := Parse(sqlCreateTable1)
	err := CreateTable(db, "", ast.Create.Table)
	assert.NotEqual(t, err, nil)
	assert.Equal(t, err.Error(), "Database test does not exist")
	CreateDatabase(db, "test")
	err = CreateTable(db, "", ast.Create.Table)
	assert.Equal(t, err, nil)
	Exists, _ := directory.Exists(db, []string{"db", "test", "test", "scheme"})
	assert.Equal(t, Exists, true)
}
