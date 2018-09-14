package opentick

import (
	"encoding/binary"
	"errors"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"strings"
)

type DataType uint32

var FdbVersion = 520

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

func CreateDatabase(db fdb.Transactor, dbName string) (err error) {
	path := []string{"db", dbName}
	exists, err1 := directory.Exists(db, path)
	if err1 != nil {
		err = err1
		return
	}
	if exists {
		err = errors.New("Database " + dbName + " already exists")
		return
	}
	_, err2 := directory.Create(db, path, nil)
	if err2 != nil {
		err = err2
		return
	}
	return
}

func ListTables(db fdb.Transactor, dbName string) (tables []string, err error) {
	path := []string{"db", dbName}
	dir, err1 := directory.Open(db, path, nil)
	if err1 != nil {
		err = err1
		return
	}
	if dir == nil {
		err = errors.New("Database " + dbName + " does not exist")
		return
	}
	tables, err = dir.List(db, nil)
	return
}

func DropDatabase(db fdb.Transactor, dbName string) (err error) {
	path := []string{"db", dbName}
	exists, err1 := directory.Exists(db, path)
	if err1 != nil {
		err = err1
		return
	}
	if !exists {
		err = errors.New("Database " + dbName + " does not exist")
		return
	}
	tables, err2 := ListTables(db, dbName)
	if err2 != nil {
		err = err2
		return
	}
	for _, tbl := range tables {
		err = DropTable(db, dbName, tbl)
		if err != nil {
			return
		}
	}
	_, err = directory.Root().Remove(db, path)
	return
}

type typeTuple struct {
	i uint32
	t DataType
}

type TableColDef struct {
	Name string
	Type DataType
}

const schemeVersion uint32 = 1

func (self *TableColDef) encode() []byte {
	var out []byte
	bn := make([]byte, 4)
	binary.BigEndian.PutUint32(bn, uint32(len(self.Name)))
	out = append(bn, []byte(self.Name)...)
	binary.BigEndian.PutUint32(bn, uint32(self.Type))
	return append(out, bn...)
}

func decodeTableColDef(bytes []byte, out *TableColDef, version uint32) []byte {
	n := binary.BigEndian.Uint32(bytes)
	bytes = bytes[4:]
	out.Name = string(bytes[:n])
	bytes = bytes[n:]
	out.Type = DataType(binary.BigEndian.Uint32(bytes))
	return bytes[4:]
}

type TableScheme struct {
	Cols []TableColDef
	Key  []uint32
}

func (self *TableScheme) encode() []byte {
	var out []byte
	bn := make([]byte, 4)
	binary.BigEndian.PutUint32(bn, schemeVersion)
	out = bn
	binary.BigEndian.PutUint32(bn, uint32(len(self.Cols)))
	out = append(out, bn...)
	for _, col := range self.Cols {
		out = append(out, col.encode()...)
	}
	binary.BigEndian.PutUint32(bn, uint32(len(self.Key)))
	out = append(out, bn...)
	for _, k := range self.Key {
		binary.BigEndian.PutUint32(bn, uint32(k))
		out = append(out, bn...)
	}
	return out
}

func decodeTableScheme(bytes []byte) TableScheme {
	v := binary.BigEndian.Uint32(bytes)
	bytes = bytes[4:]
	n := binary.BigEndian.Uint32(bytes)
	bytes = bytes[4:]
	cols := make([]TableColDef, n)
	for i := uint32(0); i < n; i++ {
		bytes = decodeTableColDef(bytes, &cols[i], v)
	}
	n = binary.BigEndian.Uint32(bytes)
	bytes = bytes[4:]
	key := make([]uint32, n)
	for i := uint32(0); i < n; i++ {
		key[i] = binary.BigEndian.Uint32(bytes)
		bytes = bytes[4:]
	}
	return TableScheme{cols, key}
}

func CreateTable(db fdb.Transactor, dbName string, ast *AstCreateTable) (err error) {
	if dbName == "" {
		dbName = ast.Name.DatabaseName()
	}
	if dbName == "" {
		err = errors.New("No database name has been specified. USE a database name, or explicitly specify databasename.tablename")
		return
	}
	exists1, err1 := directory.Exists(db, []string{"db", dbName})
	if err1 != nil {
		err = err1
		return
	}
	if !exists1 {
		err = errors.New("Database " + dbName + " does not exist")
		return
	}
	tblName := ast.Name.TableName()
	pathTable := []string{"db", dbName, tblName}
	exists2, err1 := directory.Exists(db, pathTable)
	if err1 != nil {
		err = err1
		return
	}
	if exists2 {
		err = errors.New("Table " + dbName + "." + tblName + " already exists")
		return
	}
	m := map[string]typeTuple{}
	var keyStrs []string
	tbl := TableScheme{}
	for _, f := range ast.Cols {
		if f.Key != nil {
			if keyStrs != nil {
				err = errors.New("Duplicate PRIMARY KEY")
				return
			}
			keyStrs = f.Key
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
		m[*f.Name] = typeTuple{uint32(i), t}
		tbl.Cols = append(tbl.Cols, TableColDef{*f.Name, t})
	}
	has := map[string]bool{}
	for _, k := range keyStrs {
		if _, ok := m[k]; !ok {
			err = errors.New("Unknown definition " + k + " referenced in PRIMARY KEY")
			return
		}
		if _, ok := has[k]; ok {
			err = errors.New("Duplicate definition " + k + " referenced in PRIMARY KEY")
			return
		}
		has[k] = true
		tbl.Key = append(tbl.Key, m[k].i)
	}
	if len(tbl.Key) == 0 {
		err = errors.New("PRIMARY KEY not declared")
		return
	}
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		dirTable, err2 := directory.Create(tr, pathTable, nil)
		if err2 != nil {
			err = err2
			return
		}
		dirScheme, err3 := dirTable.Create(tr, []string{"scheme"}, nil)
		if err3 != nil {
			err = err3
			return
		}
		tr.Set(fdb.Key(dirScheme.Bytes()), tbl.encode())
		return
	})
	return
}

func DropTable(db fdb.Transactor, dbName string, tblName string) (err error) {
	pathTable := []string{"db", dbName, tblName}
	dirTable, err1 := directory.Open(db, pathTable, nil)
	if err1 != nil {
		err = err1
		return
	}
	if dirTable == nil {
		err = errors.New("Table " + dbName + "." + tblName + " does not exist")
		return
	}
	dirScheme, err2 := dirTable.Open(db, []string{"scheme"}, nil)
	if err2 != nil {
		err = err2
		return
	}
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Clear(fdb.Key(dirScheme.Bytes()))
		_, err = dirTable.Remove(tr, nil)
		return
	})
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
