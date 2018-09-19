package opentick

import (
	"errors"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"strings"
)

func InsertIntoTable(db fdb.Transactor, dbName string, ast *AstInsert, values []interface{}) (err error) {
	if dbName == "" {
		dbName = ast.Table.DatabaseName()
	}
	if dbName == "" {
		err = errors.New("No database name has been specified. USE a database name, or explicitly specify databasename.tablename")
		return
	}
	scheme, err1 := GetTableScheme(db, dbName, ast.Table.TableName())
	if err1 != nil {
		err = err1
		return
	}
	iKeys := make([]int, len(scheme.Keys))
	for i := range iKeys {
		iKeys[i] = -1
	}
	iValues := make([]int, len(scheme.Values))
	for i := range iValues {
		iValues[i] = -1
	}
	nKeys := 0
	for i, colName := range ast.Cols {
		col, ok := scheme.NameMap[colName]
		if !ok {
			err = errors.New("Undefined column name " + colName)
			return
		}
		if col.IsKey {
			if iKeys[col.Pos] >= 0 {
				err = errors.New("Duplicate column name " + colName)
				return
			}
			nKeys++
			iKeys[col.Pos] = i
		} else {
			if iValues[col.Pos] >= 0 {
				err = errors.New("Duplicate column name " + colName)
				return
			}
			iValues[col.Pos] = i
		}
	}
	if nKeys < len(iKeys) {
		var missed []string
		for i, v := range iKeys {
			if v < 0 {
				missed = append(missed, scheme.Keys[i].Name)
			}
		}
		err = errors.New("Some primary keys are missing: " + strings.Join(missed, ", "))
		return
	}
	return
}

func DeleteFromTable(db fdb.Transactor, dbName string, ast *AstDelete, values []interface{}) (err error) {
	if dbName == "" {
		dbName = ast.Table.DatabaseName()
	}
	if dbName == "" {
		err = errors.New("No database name has been specified. USE a database name, or explicitly specify databasename.tablename")
		return
	}
	scheme, err1 := GetTableScheme(db, dbName, ast.Table.TableName())
	if err1 != nil {
		err = err1
		return
	}
	err = resolveWhere(&scheme, ast.Where)
	return
}

func resolveWhere(scheme *TableScheme, where *AstExpression) (err error) {
	for _, condition := range where.And {
		col, ok := scheme.NameMap[*condition.LHS]
		if !ok {
			err = errors.New("Undefined column name " + *condition.LHS)
			return
		}
		switch col.Type {
		case TinyInt:
		case SmallInt:
		case Int:
		case BigInt:
		case Double:
		case Float:
		case Timestamp:
		case Boolean:
			if *condition.Operator != "=" {
				err = errors.New("Invalid operator (" + *condition.Operator + ") for \"" + col.Name + "\" of type Boolean")
				return
			}
			if condition.RHS.Boolean == nil {
				t, v := condition.RHS.TypeValue()
				err = errors.New("Invalid " + t + " constant (" + v + ") for \"" + col.Name + "\" of type Boolean")
				return
			}
		case Text:
		}
	}
	return
}
