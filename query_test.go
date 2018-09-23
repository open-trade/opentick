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
	ast, _ := Parse("create table test.test(a int, b int, c int, d double, e bigint, primary key(a, b, c))")
	err := CreateTable(db, "", ast.Create.Table)
	ast, _ = Parse("insert into test.test(a, d) values(1, 1)")
	err = InsertIntoTable(db, "", ast.Insert, nil)
	assert.Equal(t, err.Error(), "Some primary keys are missing: b, c")
	ast, _ = Parse("insert into test.test(a, a, c) values(1, 1)")
	err = InsertIntoTable(db, "", ast.Insert, nil)
	assert.Equal(t, err.Error(), "Duplicate column name a")
	ast, _ = Parse("delete from test.test where d=1")
	err = DeleteFromTable(db, "", ast.Delete, nil)
	assert.Equal(t, err.Error(), "Invalid column d in where clause, only primary key can be used")
}
