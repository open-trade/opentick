package opentick

import (
	"errors"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"strings"
)

type DataType int

const (
	UnknowDataType DataType = iota
	TinyInt
	SmallInt
	Int
	BigInt
	Double
	Float
	Timestamp
	Boolean
	Text
)

func CreateDatabase(db fdb.Transactor, dbName string) (res bool, err error) {
	path := []string{"db", dbName}
	exists, err1 := directory.Exists(db, path)
	if err1 != nil {
		err = err1
		return
	}
	if exists {
		err = errors.New(dbName + " already exists")
		return
	}
	dir, err2 := directory.Create(db, path, nil)
	if err2 != nil {
		err = err2
		return
	}
	return true, nil
}

type typeTuple struct {
	i int
	t DataType
}

func CreateTable(db fdb.Transactor, dbName string, ast *AstCreateTable) (res bool, err error) {
	if dbName == "" {
		dbName = ast.Name.DatabaseName()
	}
	if dbName == "" {
		err = errors.New("No database name has been specified. USE a database name, or explicitly specify databasename.tablename")
		return
	}
	tblName := ast.Name.TableName()
	m := map[string]typeTuple{}
	var key []string
	for _, f := range ast.Fields {
		if f.Key != nil {
			key = f.Key
			continue
		}
		if _, ok := m[*f.Name]; ok {
			err = errors.New("Multiple definition of identifier " + *f.Name)
			return
		}
		i := len(m)
		t := parseDataType(*f.Type)
		if t == UnknowDataType {
			err = errors.New("Unknown type " + *f.Type)
			return
		}
		m[*f.Name] = typeTuple{i, t}
	}
	has := map[string]bool{}
	for _, k := range key {
		if _, ok := m[k]; !ok {
			err = errors.New("Unknown definition " + k + " referenced in PRIMARY KEY")
			return
		}
		if _, ok := has[k]; ok {
			err = errors.New("Duplicate definition " + k + " referenced in PRIMARY KEY")
			return
		}
		has[k] = true
	}
	if len(key) == 0 {
		err = errors.New("PRIMARY KEY not declared")
		return
	}
	return
}

func parseDataType(typeStr string) DataType {
	switch strings.ToUpper(typeStr) {
	case "TINYINT":
		return TinyInt
	case "SMALLINT":
		return SmallInt
	case "INT":
		return Int
	case "BIGINT":
		return BigInt
	case "DOUBLE":
		return Double
	case "FLOAT":
		return Float
	case "TIMESTAMP":
		return Timestamp
	case "BOOLEAN":
		return Boolean
	case "TEXT":
		return Text
	}
	return UnknowDataType
}
