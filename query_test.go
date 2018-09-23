package opentick

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Query(t *testing.T) {
	fdb.MustAPIVersion(FdbVersion)
	var db = fdb.MustOpenDefault()
	DropDatabase(db, "test")
	CreateDatabase(db, "test")
	ast, _ := Parse("create table test.test(a int, b int, b2 boolean, c int, d double, e bigint, primary key(a, b, b2, c))")
	err := CreateTable(db, "", ast.Create.Table)
	ast, _ = Parse("select a, b, b from test.test where a=1")
	_, err = ResolveSelect(db, "", ast.Select)
	assert.Equal(t, "Duplicate column name b", err.Error())
	ast, _ = Parse("select * from test.test where a=1")
	stmt1, err1 := ResolveSelect(db, "", ast.Select)
	assert.Equal(t, nil, err1)
	assert.Equal(t, true, stmt1.Cols == nil)
	ast, _ = Parse("select a, b from test.test where a=1")
	stmt1, err1 = ResolveSelect(db, "", ast.Select)
	assert.Equal(t, "b", stmt1.Cols[1].Name)
	assert.Equal(t, 2, len(stmt1.Cols))
	ast, _ = Parse("insert into test.test(a) values(1)")
	_, err = ResolveInsert(db, "", ast.Insert)
	assert.Equal(t, "Some primary keys are missing: b, b2, c", err.Error())
	ast, _ = Parse("insert into test.test(a, a, c) values(1, 1, 1)")
	_, err = ResolveInsert(db, "", ast.Insert)
	assert.Equal(t, "Duplicate column name a", err.Error())
	ast, _ = Parse("insert into test.test(a, a, c) values(1, 1)")
	_, err = ResolveInsert(db, "", ast.Insert)
	assert.Equal(t, "Unmatched column names/values", err.Error())
	ast, _ = Parse("insert into test.test(d) values(1)")
	_, err = ResolveInsert(db, "", ast.Insert)
	assert.Equal(t, "Invalid int64 value (1) for \"d\" of Double", err.Error())
	ast, _ = Parse("delete from test.test where d=1")
	_, err = ResolveDelete(db, "", ast.Delete)
	assert.Equal(t, "Invalid column d in where clause, only primary key can be used", err.Error())
	ast, _ = Parse("delete from test.test where a<2.2")
	_, err = ResolveDelete(db, "", ast.Delete)
	assert.Equal(t, "Invalid float64 value (2.2) for \"a\" of Int", err.Error())
	ast, _ = Parse("delete from test.test where b2<true")
	_, err = ResolveDelete(db, "", ast.Delete)
	assert.Equal(t, "Invalid operator (<) for \"b2\" of type Boolean", err.Error())
	ast, _ = Parse("delete from test.test where a<2.2")
	_, err = ResolveDelete(db, "", ast.Delete)
	assert.Equal(t, "Invalid float64 value (2.2) for \"a\" of Int", err.Error())
	ast, _ = Parse("delete from test.test where a=1 and a<1")
	_, err = ResolveDelete(db, "", ast.Delete)
	assert.Equal(t, "a cannot be restricted by more than one relation if it includes an Equal", err.Error())
	ast, _ = Parse("delete from test.test where a<=1 and a<1")
	_, err = ResolveDelete(db, "", ast.Delete)
	assert.Equal(t, "More than one restriction was found for the end bound on a", err.Error())
	ast, _ = Parse("delete from test.test where a>=1 and a>1")
	_, err = ResolveDelete(db, "", ast.Delete)
	assert.Equal(t, "More than one restriction was found for the start bound on a", err.Error())
	ast, _ = Parse("delete from test.test where b=2")
	_, err = ResolveDelete(db, "", ast.Delete)
	assert.Equal(t, "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance", err.Error())
	ast, _ = Parse("delete from test.test where a<2 and b=2")
	_, err = ResolveDelete(db, "", ast.Delete)
	assert.Equal(t, "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance", err.Error())
	ast, _ = Parse("delete from test.test where a=2 and b=2 and b2=?")
	stmt2, err2 := ResolveDelete(db, "", ast.Delete)
	assert.Equal(t, nil, err2)
	assert.Equal(t, uint32(1), stmt2.NumPlaceholders)
	assert.Equal(t, 4, len(stmt2.Scheme.Keys))
}

func Benchmark_ResolveDelete(b *testing.B) {
	b.StopTimer()
	fdb.MustAPIVersion(FdbVersion)
	var db = fdb.MustOpenDefault()
	DropDatabase(db, "test")
	CreateDatabase(db, "test")
	ast, _ := Parse("create table test.test(a int, b int, c int, d double, e bigint, primary key(a, b, c))")
	CreateTable(db, "", ast.Create.Table)
	ast, _ = Parse("delete from test.test where a=2 and b=2 and c<?")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := ResolveDelete(db, "", ast.Delete)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_ResolveInsert(b *testing.B) {
	b.StopTimer()
	fdb.MustAPIVersion(FdbVersion)
	var db = fdb.MustOpenDefault()
	DropDatabase(db, "test")
	CreateDatabase(db, "test")
	ast, _ := Parse("create table test.test(a int, b int, c int, d double, e bigint, primary key(a, b, c))")
	CreateTable(db, "", ast.Create.Table)
	ast, _ = Parse("insert into test.test(a, b, c, d) values(1, 2, ?, 1.2)")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := ResolveInsert(db, "", ast.Insert)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_ResolveSelect(b *testing.B) {
	b.StopTimer()
	fdb.MustAPIVersion(FdbVersion)
	var db = fdb.MustOpenDefault()
	DropDatabase(db, "test")
	CreateDatabase(db, "test")
	ast, _ := Parse("create table test.test(a int, b int, c int, d double, e bigint, primary key(a, b, c))")
	CreateTable(db, "", ast.Create.Table)
	ast, _ = Parse("select a, b, c, d from test.test where a=1 and b=2 and c<2 and c>1")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := ResolveSelect(db, "", ast.Select)
		if err != nil {
			b.Fatal(err)
		}
	}
}
