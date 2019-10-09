package opentick

import (
	"encoding/binary"
	"errors"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"strings"
	"sync"
)

type DataType uint32

var FdbVersion = 520
var TableSchemaMap = sync.Map{}

const (
	TinyInt DataType = iota
	SmallInt
	Int
	BigInt
	Double
	Float
	Timestamp
	Boolean
	Text
)

var typeNames = []string{"TinyInt", "SmallInt", "Int", "BigInt", "Double", "Float", "Timestamp", "Boolean", "Text"}

func (self *DataType) Name() string {
	i := int(*self)
	if i >= len(typeNames) {
		return ""
	}
	return typeNames[i]
}

func HasDatabase(db fdb.Transactor, dbName string) (bool, error) {
	path := []string{"db", dbName}
	return directory.Exists(db, path)
}

func HasTable(db fdb.Transactor, dbName string, tblName string) (bool, error) {
	path := []string{"db", dbName, tblName}
	return directory.Exists(db, path)
}

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
	CreateAdj(db, dbName)
	return
}

func ListDatabases(db fdb.Transactor) (dbNames []string, err error) {
	path := []string{"db"}
	dir, err1 := directory.Open(db, path, nil)
	if err1 != nil {
		err = err1
		return
	}
	if dir == nil {
		err = errors.New("Database dir does not exist")
		return
	}
	dbNames, err = dir.List(db, nil)
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
	Name   string
	Type   DataType
	IsKey  bool
	PosCol uint32
	Pos    uint32 // position in Key or Values
}

func NewTableColDef(name string, t DataType) (tbl *TableColDef) {
	tbl = &TableColDef{}
	tbl.Name = name
	tbl.Type = t
	return
}

const schemaVersion uint32 = 1

func (self *TableColDef) encode() []byte {
	var out []byte
	var tmp [4]byte
	bn := tmp[:]
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

type TableSchema struct {
	DbName  string
	TblName string
	Cols    []*TableColDef
	Keys    []*TableColDef
	Values  []*TableColDef
	NameMap map[string]*TableColDef
	Dir     directory.DirectorySubspace
}

func NewTableSchema(cols []*TableColDef, keys []int) (tbl TableSchema) {
	tbl.Cols = cols
	tbl.Keys = make([]*TableColDef, len(keys))
	for i := range keys {
		tbl.Keys[i] = cols[keys[i]]
	}
	tbl.fill()
	return
}

func (self *TableSchema) fill() {
	self.Values = make([]*TableColDef, len(self.Cols)-len(self.Keys))
	for i, col := range self.Keys {
		col.IsKey = true
		col.Pos = uint32(i)
	}
	n := 0
	self.NameMap = make(map[string]*TableColDef)
	for i, col := range self.Cols {
		col.PosCol = uint32(i)
		self.NameMap[col.Name] = col
		if !col.IsKey {
			self.Values[n] = col
			col.Pos = uint32(n)
			n++
		}
	}
}

func (self *TableSchema) encode() []byte {
	var out []byte
	var tmp [4]byte
	bn := tmp[:]
	binary.BigEndian.PutUint32(bn, schemaVersion)
	out = bn
	binary.BigEndian.PutUint32(bn, uint32(len(self.Cols)))
	out = append(out, bn...)
	for _, col := range self.Cols {
		out = append(out, col.encode()...)
	}
	binary.BigEndian.PutUint32(bn, uint32(len(self.Keys)))
	out = append(out, bn...)
	for _, k := range self.Keys {
		binary.BigEndian.PutUint32(bn, uint32(k.PosCol))
		out = append(out, bn...)
	}
	return out
}

func decodeTableSchema(bytes []byte) *TableSchema {
	v := binary.BigEndian.Uint32(bytes)
	bytes = bytes[4:]
	n := binary.BigEndian.Uint32(bytes)
	bytes = bytes[4:]
	cols := make([]*TableColDef, n)
	for i := uint32(0); i < n; i++ {
		cols[i] = &TableColDef{}
		bytes = decodeTableColDef(bytes, cols[i], v)
	}
	n = binary.BigEndian.Uint32(bytes)
	bytes = bytes[4:]
	keys := make([]*TableColDef, n)
	for i := uint32(0); i < n; i++ {
		keys[i] = cols[int(binary.BigEndian.Uint32(bytes))]
		bytes = bytes[4:]
	}
	tbl := TableSchema{Cols: cols, Keys: keys}
	tbl.fill()
	return &tbl
}

func CreateAdj(db fdb.Transactor, dbName string) (err error) {
	stmt, err1 := Parse(`
	create table _adj_(
		sec int,
  	tm timestamp,
		px double,
		vol double,
		primary key (sec, tm)
	)
  `)
	if err1 != nil {
		return err1
	}
	err = CreateTable(db, dbName, stmt.Create.Table)
	return
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
	tbl := TableSchema{}
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
		m[*f.Name] = typeTuple{uint32(i), t}
		tbl.Cols = append(tbl.Cols, NewTableColDef(*f.Name, t))
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
		tbl.Keys = append(tbl.Keys, tbl.Cols[m[k].i])
	}
	if len(tbl.Keys) == 0 {
		err = errors.New("PRIMARY KEY not declared")
		return
	}
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		dirTable, err2 := directory.Create(tr, pathTable, nil)
		if err2 != nil {
			err = err2
			return
		}
		dirSchema, err3 := dirTable.Create(tr, []string{"scheme"}, nil)
		if err3 != nil {
			err = err3
			return
		}
		tbl.fill()
		tr.Set(dirSchema, tbl.encode())
		return
	})
	return
}

func openTable(db fdb.Transactor, dbName string, tblName string) (dirTable directory.DirectorySubspace, dirSchema directory.DirectorySubspace, err error) {
	pathTable := []string{"db", dbName, tblName}
	var exists bool
	exists, err = directory.Exists(db, pathTable)
	if err != nil {
		return
	}
	if !exists {
		err = errors.New("Table " + dbName + "." + tblName + " does not exists")
		return
	}
	dirTable, err = directory.Open(db, pathTable, nil)
	if err != nil {
		return
	}
	dirSchema, err = dirTable.Open(db, []string{"scheme"}, nil)
	return
}

func DropTable(db fdb.Transactor, dbName string, tblName string) (err error) {
	TableSchemaMap.Delete(dbName + "." + tblName)
	dirTable, dirSchema, err1 := openTable(db, dbName, tblName)
	if err1 != nil {
		err = err1
		return
	}
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Clear(dirSchema)
		_, err = dirTable.Remove(tr, nil)
		tr.ClearRange(dirTable)
		return
	})
	return
}

func RenameTableField(db fdb.Transactor, tbl *TableSchema, from string, to string) (err error) {
	TableSchemaMap.Delete(tbl.DbName + "." + tbl.TblName)
	// create new table schema to modify rather than modify older
	tbl, err = GetTableSchema(db, tbl.DbName, tbl.TblName)
	if err != nil {
		return
	}
	TableSchemaMap.Delete(tbl.DbName + "." + tbl.TblName)
	_, dirSchema, err1 := openTable(db, tbl.DbName, tbl.TblName)
	if err1 != nil {
		return err1
	}
	col, ok := tbl.NameMap[from]
	if !ok {
		return errors.New("Column " + from + " does not exist")
	}
	if _, ok := tbl.NameMap[to]; ok {
		return errors.New("Column " + to + " already exists")
	}
	col.Name = to // potential bug
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.Set(dirSchema, tbl.encode())
		return
	})
	return
}

func parseDataType(typeStr string) (d DataType) {
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
	return
}

func GetTableSchema(db fdb.Transactor, dbName string, tblName string) (tbl *TableSchema, err error) {
	fullName := dbName + "." + tblName
	tmp, _ := TableSchemaMap.Load(fullName)
	if tmp != nil {
		tbl = tmp.(*TableSchema)
		return
	}
	dirTable, dirSchema, err1 := openTable(db, dbName, tblName)
	if err1 != nil {
		err = err1
		return
	}
	ret, err1 := db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		ret = decodeTableSchema(tr.Get(dirSchema).MustGet())
		return
	})
	if err1 != nil {
		err = err1
		return
	}
	tbl = ret.(*TableSchema)
	tbl.Dir = dirTable
	tbl.DbName = dbName
	tbl.TblName = tblName
	TableSchemaMap.Store(fullName, tbl)
	return
}
