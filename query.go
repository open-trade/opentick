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

func ResolveSelect(db fdb.Transactor, dbName string, ast *AstSelect) (stmt selectStmt, err error) {
	if dbName == "" {
		dbName = ast.Table.DatabaseName()
	}
	if dbName == "" {
		err = errors.New("No database name has been specified. USE a database name, or explicitly specify databasename.tablename")
		return
	}
	stmt.Scheme, err = GetTableScheme(db, dbName, ast.Table.TableName())
	scheme := stmt.Scheme
	if err != nil {
		return
	}
	stmt.Conds, stmt.NumPlaceholders, err = resolveWhere(stmt.Scheme, ast.Where)
	if err != nil {
		return
	}
	if ast.Selected.All != nil {
		return
	}
	used := make([]bool, len(scheme.Cols))
	stmt.Cols = make([]*TableColDef, len(ast.Selected.Cols))
	for j, colName := range ast.Selected.Cols {
		col, ok := scheme.NameMap[colName]
		if !ok {
			err = errors.New("Undefined column name " + colName)
			return
		}
		i := col.PosCol
		if used[i] {
			err = errors.New("Duplicate column name " + colName)
			return
		}
		used[i] = true
		stmt.Cols[j] = col
	}
	return
}

type selectStmt struct {
	Scheme          *TableScheme
	Conds           []condition    // len(Scheme.Keys)
	Cols            []*TableColDef // nil or len(ast.Selected.Cols)
	NumPlaceholders uint32
}

func ResolveInsert(db fdb.Transactor, dbName string, ast *AstInsert) (stmt insertStmt, err error) {
	if dbName == "" {
		dbName = ast.Table.DatabaseName()
	}
	if dbName == "" {
		err = errors.New("No database name has been specified. USE a database name, or explicitly specify databasename.tablename")
		return
	}
	stmt.Scheme, err = GetTableScheme(db, dbName, ast.Table.TableName())
	scheme := stmt.Scheme
	if err != nil {
		return
	}
	if len(ast.Cols) != len(ast.Values) {
		err = errors.New("Unmatched column names/values")
		return
	}
	stmt.Values = make([]interface{}, len(scheme.Cols))
	for j, colName := range ast.Cols {
		col, ok := scheme.NameMap[colName]
		if !ok {
			err = errors.New("Undefined column name " + colName)
			return
		}
		i := col.PosCol
		if stmt.Values[i] != nil {
			err = errors.New("Duplicate column name " + colName)
			return
		}
		if ast.Values[j].Placeholder != nil {
			stmt.Values[i] = placeholder(stmt.NumPlaceholders)
			stmt.NumPlaceholders++
			continue
		}
		stmt.Values[i], err = validValue(col, ast.Values[j].Value())
		if err != nil {
			return
		}
	}
	var missed []string
	for _, col := range scheme.Keys {
		if stmt.Values[col.PosCol] == nil {
			missed = append(missed, col.Name)
		}
	}
	if missed != nil {
		err = errors.New("Some primary keys are missing: " + strings.Join(missed, ", "))
		return
	}
	return
}

type insertStmt struct {
	Scheme          *TableScheme
	Values          []interface{} // len(Scheme.Cols)
	NumPlaceholders uint32
}

func ResolveDelete(db fdb.Transactor, dbName string, ast *AstDelete) (stmt deleteStmt, err error) {
	if dbName == "" {
		dbName = ast.Table.DatabaseName()
	}
	if dbName == "" {
		err = errors.New("No database name has been specified. USE a database name, or explicitly specify databasename.tablename")
		return
	}
	stmt.Scheme, err = GetTableScheme(db, dbName, ast.Table.TableName())
	if err != nil {
		return
	}
	stmt.Conds, stmt.NumPlaceholders, err = resolveWhere(stmt.Scheme, ast.Where)
	return
}

type deleteStmt struct {
	Scheme          *TableScheme
	Conds           []condition // len(Scheme.Keys)
	NumPlaceholders uint32
}

type placeholder uint32

type condition struct {
	Equal interface{}
	End   [2]interface{}
	Start [2]interface{}
}

func (self *condition) IsEmpty() bool {
	return self.Equal == nil && self.End[0] == nil && self.Start[0] == nil
}

func (self *condition) IsRange() bool {
	return self.End[0] != nil || self.Start[0] != nil
}

func resolveWhere(scheme *TableScheme, where *AstExpression) (conds []condition, numPlaceholder uint32, err error) {
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
			rhs = placeholder(numPlaceholder)
			numPlaceholder++
		} else {
			rhs, err = validValue(col, cond.RHS.Value())
			if err != nil {
				return
			}
		}
		if conds[col.Pos].Equal != nil {
			err = errors.New(col.Name + " cannot be restricted by more than one relation if it includes an Equal")
			return
		}
		switch op {
		case "=":
			if conds[col.Pos].IsRange() {
				err = errors.New(col.Name + " cannot be restricted by more than one relation if it includes an Equal")
				return
			}
			conds[col.Pos].Equal = rhs
		case "<":
			if conds[col.Pos].End[0] != nil {
				err = errors.New("More than one restriction was found for the end bound on " + col.Name)
				return
			}
			conds[col.Pos].End[0] = rhs
		case "<=":
			if conds[col.Pos].End[0] != nil {
				err = errors.New("More than one restriction was found for the end bound on " + col.Name)
				return
			}
			conds[col.Pos].End[0] = rhs
			conds[col.Pos].End[1] = true
		case ">":
			if conds[col.Pos].Start[0] != nil {
				err = errors.New("More than one restriction was found for the start bound on " + col.Name)
				return
			}
			conds[col.Pos].Start[0] = rhs
		case ">=":
			if conds[col.Pos].Start[0] != nil {
				err = errors.New("More than one restriction was found for the start bound on " + col.Name)
				return
			}
			conds[col.Pos].Start[0] = rhs
			conds[col.Pos].Start[1] = true
		}
	}
	hasRange := false
	hasEmpty := false
	for i := range conds {
		isRange := conds[i].IsRange()
		isEmpty := conds[i].IsEmpty()
		if !isEmpty {
			if hasEmpty || hasRange {
				err = errors.New("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance")
				return
			}
		} else {
			hasEmpty = true
		}
		if isRange {
			hasRange = true
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
				dt.Nanosecond = uint32(v1 & 0xFFFFFFFF)
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
		dt.Nanosecond = uint32(time1.Nanosecond())
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
