package opentick

import (
	"errors"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"math"
	"reflect"
	"strings"
	"time"
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
	_, err2 := resolveWhere(&scheme, ast.Where)
	if err2 != nil {
		err = err2
		return
	}
	return
}

type placeholder bool

type condition struct {
	Equal interface{}
	Upper [2]interface{}
	Lower [2]interface{}
}

func resolveWhere(scheme *TableScheme, where *AstExpression) (conds []condition, err error) {
	conds = make([]condition, len(scheme.Keys))
	for _, cond := range where.And {
		col, ok := scheme.NameMap[*cond.LHS]
		if !ok {
			err = errors.New("Undefined column name " + *cond.LHS)
			return
		}
		if !col.IsKey {
			err = errors.New("Invalid column " + col.Name + " in where clause, only primary key can be used")
			return
		}
		op := *cond.Operator
		if col.Type == Boolean && op != "=" {
			err = errors.New("Invalid operator (" + *cond.Operator + ") for \"" + col.Name + "\" of type Boolean")
			return
		}
		var rhs interface{}
		if cond.RHS.Placeholder != nil {
			rhs = placeholder(true)
		} else {
			rhs, err = validValue(col, cond.RHS.Value())
			if err != nil {
				return
			}
		}
		switch op {
		case "=":
			conds[col.Pos].Equal = rhs
		case "<":
			conds[col.Pos].Lower[0] = rhs
		case "<=":
			conds[col.Pos].Lower[0] = rhs
			conds[col.Pos].Lower[1] = true
		case ">":
			conds[col.Pos].Upper[0] = rhs
		case ">=":
			conds[col.Pos].Upper[0] = rhs
			conds[col.Pos].Upper[1] = true
		}
	}
	return
}

func validValue(col *TableColDef, v interface{}) (ret interface{}, err error) {
	switch col.Type {
	case TinyInt, SmallInt, Int, BigInt:
		v1, ok := v.(int64)
		if !ok {
			goto hasError
		}
		switch col.Type {
		case TinyInt:
			if v1 > math.MaxInt8 {
				v1 = math.MaxInt8
			} else if v1 < math.MinInt8 {
				v1 = math.MinInt8
			}
			ret = int8(v1)
			return
		case SmallInt:
			if v1 > math.MaxInt16 {
				v1 = math.MaxInt16
			} else if v1 < math.MinInt16 {
				v1 = math.MinInt16
			}
			ret = int16(v1)
			return
		case Int:
			if v1 > math.MaxInt32 {
				v1 = math.MaxInt32
			} else if v1 < math.MinInt32 {
				v1 = math.MinInt32
			}
			ret = int32(v1)
			return
		case BigInt:
			ret = v1
			return
		}
	case Double, Float:
		v1, ok := v.(float64)
		if !ok {
			goto hasError
		}
		switch col.Type {
		case Double:
			ret = v1
			return
		case Float:
			ret = float32(v1)
			return
		}
	case Boolean:
		v1, ok := v.(bool)
		if !ok {
			goto hasError
		}
		ret = v1
		return
	case Timestamp:
		var dt Datetime
		v1, ok1 := v.(int64)
		if ok1 {
			if (v1 >> 32) == 0 {
				dt.Second = v1
			} else {
				dt.Second = v1 >> 32
				dt.Nanosecond = int(v1 & 0xFFFFFFFF)
			}
			ret = dt
			return
		}
		v2, ok2 := v.(string)
		if !ok2 {
			goto hasError
		}
		time1, err1 := time.Parse(time.RFC3339, v2)
		if err1 != nil {
			goto hasError
		}
		dt.Second = time1.Unix()
		dt.Nanosecond = time1.Nanosecond()
		ret = dt
		return
	case Text:
		v1, ok := v.(string)
		if !ok {
			goto hasError
		}
		ret = v1
		return
	}
hasError:
	err = errors.New("Invalid " + fmt.Sprint(reflect.TypeOf(v)) + " value (" + fmt.Sprint(v) + ") for \"" + col.Name + "\" of " + col.Type.Name())
	return
}
