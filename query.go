package opentick

import (
	"errors"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func Resolve(db fdb.Transactor, dbName string, ast *Ast) (stmt interface{}, err error) {
	if ast.Select != nil {
		return resolveSelect(db, dbName, ast.Select)
	} else if ast.Insert != nil {
		return resolveInsert(db, dbName, ast.Insert)
	} else if ast.Delete != nil {
		return resolveDelete(db, dbName, ast.Delete)
	}
	err = errors.New("Only select/insert/delete can be resolved")
	return
}

func ExecuteStmt(db fdb.Transactor, stmt interface{}, args []interface{}) (res [][]interface{}, err error) {
	if stmt2, ok := stmt.(insertStmt); ok {
		err = executeInsert(db, &stmt2, args)
		return
	}
	if stmt2, ok := stmt.(selectStmt); ok {
		return executeSelect(db, &stmt2, args)
	}
	if stmt2, ok := stmt.(deleteStmt); ok {
		err = executeDelete(db, &stmt2, args)
		return
	}
	err = errors.New("Invalid statement")
	return
}

func Execute(db fdb.Transactor, dbName string, sql string, args []interface{}) (res [][]interface{}, err error) {
	ast, err1 := Parse(sql)
	if err1 != nil {
		return nil, err1
	}

	if ast.Create != nil {
		if ast.Create.Database != nil {
			if ast.Create.Database.IfNotExists != nil {
				exists, err1 := HasDatabase(db, *ast.Create.Database.Name)
				if err1 != nil {
					err = err1
					return
				}
				if exists {
					return
				}
			}
			err = CreateDatabase(db, *ast.Create.Database.Name)
		} else if ast.Create.Table != nil {
			if ast.Create.Table.IfNotExists != nil {
				if dbName == "" {
					dbName = ast.Create.Table.Name.DatabaseName()
				}
				exists, err1 := HasTable(db, dbName, ast.Create.Table.Name.TableName())
				if err1 != nil {
					err = err1
					return
				}
				if exists {
					return
				}
			}
			err = CreateTable(db, dbName, ast.Create.Table)
		}
	} else if ast.Drop != nil {
		if ast.Drop.Database != nil {
			err = DropDatabase(db, *ast.Drop.Database)
			adjCache.clear(*ast.Drop.Database)
		} else if ast.Drop.Table != nil {
			if dbName == "" {
				dbName = ast.Drop.Table.DatabaseName()
			}
			if ast.Drop.Table.TableName() == "adj" {
				adjCache.clear(dbName)
			}
			err = DropTable(db, dbName, ast.Drop.Table.TableName())
		}
	} else {
		stmt, err1 := Resolve(db, dbName, ast)
		if err1 != nil {
			err = err1
			return
		}
		return ExecuteStmt(db, stmt, args)
	}
	return
}

func executeSelect(db fdb.Transactor, stmt *selectStmt, args []interface{}) (res [][]interface{}, err error) {
	sel, conds, err1 := executeWhere(db, stmt, args)
	if err1 != nil {
		err = err1
		return
	}
	if bytes, ok := sel.([]byte); ok {
		tmp, err1 := db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
			ret = tr.Get(fdb.Key(bytes)).MustGet()
			return
		})
		if err1 != nil {
			err = err1
			return
		}
		bytes = tmp.([]byte)
		if len(bytes) == 0 {
			return
		}
		value, err2 := tuple.Unpack(bytes)
		if err2 != nil {
			err = errors.New("Internal errror: " + err2.Error())
			return
		}
		res = []([]interface{}){make([]interface{}, len(stmt.Cols))}
		applyFuncOne(db, stmt, value)
		for i, col := range stmt.Cols {
			if col.IsKey {
				res[0][i] = conds[col.Pos].Equal
			} else if int(col.Pos) < len(value) {
				res[0][i] = value[col.Pos]
			}
		}
		return
	}
	kr := sel.(fdb.KeyRange)
	tmp, err2 := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return tr.GetRange(kr, fdb.RangeOptions{Limit: stmt.Limit, Reverse: stmt.Reverse}).GetSliceWithError()
	})
	if err2 != nil {
		err = err2
		return
	}
	if tmp == nil {
		return
	}
	recs := tmp.([]fdb.KeyValue)
	if len(recs) == 0 {
		return
	}
	tmpRes := make([][2]tuple.Tuple, len(recs))
	for i, rec := range recs {
		key, err1 := stmt.Scheme.Dir.Unpack(rec.Key)
		if err1 != nil {
			err = errors.New("Internal errror: " + err1.Error())
			return
		}
		value, err2 := tuple.Unpack(rec.Value)
		if err2 != nil {
			err = errors.New("Internal errror: " + err2.Error())
			return
		}
		tmpRes[i] = [2]tuple.Tuple{key, value}
	}
	applyFunc(db, stmt, tmpRes)
	res = make([]([]interface{}), len(recs))
	for i, tmp := range tmpRes {
		key, value := tmp[0], tmp[1]
		row := make([]interface{}, len(stmt.Cols))
		res[i] = row
		for j, col := range stmt.Cols {
			if col.IsKey {
				if int(col.Pos) < len(key) {
					row[j] = key[col.Pos]
				}
			} else if int(col.Pos) < len(value) {
				row[j] = value[col.Pos]
			}
		}
	}
	return
}

func executeDelete(db fdb.Transactor, stmt *deleteStmt, args []interface{}) (err error) {
	if stmt.Scheme.TblName == "adj" {
		adjCache.clear(stmt.Scheme.DbName)
	}
	tmp, _, err1 := executeWhere(db, stmt, args)
	if err1 != nil {
		err = err1
		return
	}
	if bytes, ok := tmp.([]byte); ok {
		_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
			tr.Clear(fdb.Key(bytes))
			return
		})
		return
	}
	kr := tmp.(fdb.KeyRange)
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.ClearRange(kr)
		return
	})
	return
}

func executeWhere(db fdb.Transactor, stmt whereStmt, args []interface{}) (res interface{}, conds []condition, err error) {
	np := stmt.GetNumPlaceholders()
	if np != len(args) {
		err = errors.New("Expected " + strconv.FormatInt(int64(np), 10) + " arguments, got " + strconv.FormatInt(int64(len(args)), 10))
		return
	}
	conds = stmt.GetConds()
	scheme := stmt.GetScheme()
	if len(args) > 0 {
		conds, err = validateConditionArgs(scheme, conds, args)
		if err != nil {
			return
		}
	}
	var sub subspace.Subspace
	sub = scheme.Dir
	n := len(conds) - 1
	if n > 0 {
		for i := range conds[:n] {
			sub = sub.Sub(conds[i].Equal)
		}
	}
	c := &conds[n]
	if c.Equal != nil && len(conds) == len(scheme.Keys) {
		res = sub.Sub(c.Equal).Bytes()
		return
	}
	kr := fdb.KeyRange{}
	if c.Equal != nil {
		a, b := sub.Sub(c.Equal).FDBRangeKeys()
		kr.Begin = a
		kr.End = b
	} else {
		if c.Start[0] != nil {
			k := sub.Sub(c.Start[0])
			if c.Start[1] == nil {
				kr.Begin = fdb.Key(append(k.Bytes(), 0x1))
			} else {
				kr.Begin = k
			}
		} else {
			kr.Begin = fdb.Key(append(sub.Bytes(), 0x00))
		}
		if c.End[0] != nil {
			k := sub.Sub(c.End[0])
			if c.End[1] == nil {
				kr.End = k
			} else {
				kr.End = fdb.Key(append(k.Bytes(), 0x1))
			}
		} else {
			kr.End = fdb.Key(append(sub.Bytes(), 0xFF))
		}
	}
	res = kr
	return
}

func BatchInsert(db fdb.Transactor, stmt *insertStmt, argsArray [][]interface{}) (err error) {
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		for _, args := range argsArray {
			var parts [2][]tuple.TupleElement
			err = prepareInsert(stmt, args, &parts)
			if err != nil {
				return
			}
			tr.Set(stmt.Scheme.Dir.Pack(tuple.Tuple(parts[0])), tuple.Tuple(parts[1]).Pack())
		}
		return
	})
	return
}

func prepareInsert(stmt *insertStmt, args []interface{}, parts *[2][]tuple.TupleElement) (err error) {
	if stmt.NumPlaceholders != len(args) {
		err = errors.New("Expected " + strconv.FormatInt(int64(stmt.NumPlaceholders), 10) + " arguments, got " + strconv.FormatInt(int64(len(args)), 10))
		return
	}
	values := stmt.Values
	if len(args) > 0 {
		values = make([]interface{}, len(stmt.Values))
		copy(values, stmt.Values)
		for i := range values {
			if p, ok := values[i].(placeholder); ok {
				values[i], err = validateValue(stmt.Scheme.Cols[i], args[int(p)])
				if err != nil {
					return
				}
			}
		}
	}
	for i, cols := range [2]([]*TableColDef){stmt.Scheme.Keys, stmt.Scheme.Values} {
		parts[i] = make([]tuple.TupleElement, len(cols))
		for _, col := range cols {
			v := values[col.PosCol]
			parts[i][col.Pos] = tuple.TupleElement(v)
		}
	}
	return
}

func executeInsert(db fdb.Transactor, stmt *insertStmt, args []interface{}) (err error) {
	if stmt.Scheme.TblName == "adj" {
		adjCache.clear(stmt.Scheme.DbName)
	}
	argsArray := [1][]interface{}{args}
	return BatchInsert(db, stmt, argsArray[:])
}

func resolveSelect(db fdb.Transactor, dbName string, ast *AstSelect) (stmt selectStmt, err error) {
	stmt.Scheme, err = getTableScheme(db, dbName, ast.Table)
	scheme := stmt.Scheme
	if err != nil {
		return
	}
	stmt.Conds, stmt.NumPlaceholders, err = resolveWhere(stmt.Scheme, ast.Where)
	if err != nil {
		return
	}
	if ast.Limit != nil {
		stmt.Limit = int(*ast.Limit)
		if stmt.Limit < 0 {
			stmt.Limit = -stmt.Limit
			stmt.Reverse = true
		}
	}
	if ast.Selected.All != nil {
		stmt.Cols = scheme.Cols
		return
	}
	used := make([]bool, len(scheme.Cols))
	stmt.Cols = make([]*TableColDef, len(ast.Selected.Cols))
	stmt.Funcs = make([]*selectFunc, len(ast.Selected.Cols))
	for j, col := range ast.Selected.Cols {
		colName := col.Name
		var funcName *string
		var params []AstValue
		if colName == nil {
			colName = col.Func.Col
			funcName = col.Func.Name
			params = col.Func.Params
		}
		col, ok := scheme.NameMap[*colName]
		if !ok {
			err = errors.New("Undefined column name " + *colName)
			return
		}
		i := col.PosCol
		if used[i] {
			err = errors.New("Duplicate column name " + *colName)
			return
		}
		used[i] = true
		stmt.Cols[j] = col
		if funcName != nil {
			tmp := strings.ToLower(*funcName)
			funcName = &tmp
			if tmp == "adj" {
				tmp = strings.ToLower(col.Name)
				if strings.Contains(tmp, "qty") || strings.Contains(tmp, "vol") || strings.Contains(tmp, "size") {
					tmp = "adj_vol"
				} else {
					tmp = "adj_px"
				}
				funcName = &tmp
			}
			if tmp == "adj_vol" || tmp == "adj_px" {
				if params != nil && (len(params) > 1 || params[0].Boolean == nil) {
					err = errors.New("adj only accept one optional bool params")
					return
				}
			}
			stmt.Funcs[j] = &selectFunc{*funcName, params}
		}
	}
	err = getAdjTuples(&stmt)
	return
}

type whereStmt interface {
	GetNumPlaceholders() int
	GetConds() []condition
	GetScheme() *TableScheme
}

type adjTuple struct {
	Pos      int
	Adj      int // 1: px, 2: vol
	Backward bool
}

type selectFunc struct {
	Name   string
	Params []AstValue
}

type selectStmt struct {
	Scheme          *TableScheme
	Conds           []condition    // <= len(Scheme.Keys)
	Cols            []*TableColDef // nil or len(ast.Selected.Cols)
	Funcs           []*selectFunc
	NumPlaceholders int
	Limit           int
	Reverse         bool
	Adjs            []adjTuple
}

func (self *selectStmt) GetNumPlaceholders() int {
	return self.NumPlaceholders
}

func (self *selectStmt) GetConds() []condition {
	return self.Conds
}

func (self *selectStmt) GetScheme() *TableScheme {
	return self.Scheme
}

func resolveInsert(db fdb.Transactor, dbName string, ast *AstInsert) (stmt insertStmt, err error) {
	stmt.Scheme, err = getTableScheme(db, dbName, ast.Table)
	scheme := stmt.Scheme
	if err != nil {
		return
	}
	if ast.Cols == nil {
		for _, col := range stmt.Scheme.Cols {
			ast.Cols = append(ast.Cols, col.Name)
		}
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
		stmt.Values[i], err = validateValue(col, ast.Values[j].Value())
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
	NumPlaceholders int
}

func resolveDelete(db fdb.Transactor, dbName string, ast *AstDelete) (stmt deleteStmt, err error) {
	stmt.Scheme, err = getTableScheme(db, dbName, ast.Table)
	if err != nil {
		return
	}
	stmt.Conds, stmt.NumPlaceholders, err = resolveWhere(stmt.Scheme, ast.Where)
	return
}

type deleteStmt struct {
	Scheme          *TableScheme
	Conds           []condition // <= len(Scheme.Keys)
	NumPlaceholders int
}

func (self *deleteStmt) GetNumPlaceholders() int {
	return self.NumPlaceholders
}

func (self *deleteStmt) GetConds() []condition {
	return self.Conds
}

func (self *deleteStmt) GetScheme() *TableScheme {
	return self.Scheme
}

type placeholder int

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

func resolveWhere(scheme *TableScheme, where *AstExpression) (conds []condition, numPlaceholder int, err error) {
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
			rhs, err = validateValue(col, cond.RHS.Value())
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
	n := 0
	for i := range conds {
		isRange := conds[i].IsRange()
		isEmpty := conds[i].IsEmpty()
		if !isEmpty {
			if hasEmpty || hasRange {
				err = errors.New("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance")
				return
			}
			n++
		} else {
			hasEmpty = true
		}
		if isRange {
			hasRange = true
		}
	}
	conds = conds[:n]
	return
}

func getInt(v interface{}) (ret int64, ok bool) {
	if v1, ok1 := v.(int); ok1 {
		return int64(v1), true
	}
	if v1, ok1 := v.(int64); ok1 {
		return v1, true
	}
	if v1, ok1 := v.(int16); ok1 {
		return int64(v1), true
	}
	if v1, ok1 := v.(int8); ok1 {
		return int64(v1), true
	}
	return
}

func getFloat(v interface{}) (ret float64, ok bool) {
	if v1, ok1 := v.(float64); ok1 {
		return v1, true
	}
	if v1, ok1 := v.(float32); ok1 {
		return float64(v1), true
	}
	if v1, ok1 := getInt(v); ok1 {
		return float64(v1), true
	}
	return
}

func validateValue(col *TableColDef, v interface{}) (ret interface{}, err error) {
	switch col.Type {
	case TinyInt, SmallInt, Int, BigInt:
		var v1 int64
		if v2, ok := getInt(v); ok {
			v1 = v2
		} else {
			goto hasError
		}
		switch col.Type {
		case TinyInt:
			if v1 > math.MaxInt8 {
				v1 = math.MaxInt8
			} else if v1 < math.MinInt8 {
				v1 = math.MinInt8
			}
		case SmallInt:
			if v1 > math.MaxInt16 {
				v1 = math.MaxInt16
			} else if v1 < math.MinInt16 {
				v1 = math.MinInt16
			}
		case Int:
			if v1 > math.MaxInt32 {
				v1 = math.MaxInt32
			} else if v1 < math.MinInt32 {
				v1 = math.MinInt32
			}
		}
		ret = v1
	case Double, Float:
		var v1 float64
		switch v.(type) {
		case int64:
			v1 = float64(v.(int64))
		case int:
			v1 = float64(v.(int))
		case float64:
			v1 = v.(float64)
		default:
			goto hasError
		}
		switch col.Type {
		case Double:
			ret = v1
		case Float:
			ret = float32(v1)
		}
	case Boolean:
		v1, ok := v.(bool)
		if !ok {
			goto hasError
		}
		ret = v1
	case Timestamp:
		switch v.(type) {
		case int64:
			ret = tuple.Tuple{v.(int64), 0}
			return
		case int:
			ret = tuple.Tuple{v.(int), 0}
			return
		case []interface{}:
			v1 := v.([]interface{})
			if len(v1) == 2 {
				if v3, ok3 := getInt(v1[0]); ok3 {
					if v4, ok4 := getInt(v1[1]); ok4 {
						ret = tuple.Tuple{v3, v4}
						return
					}
				}
			}
		case string:
			time1, err1 := time.Parse(time.RFC3339, v.(string))
			if err1 == nil {
				ret = tuple.Tuple{time1.Unix(), int(time1.Nanosecond())}
				return
			}
		}
		goto hasError
	case Text:
		v1, ok := v.(string)
		if !ok {
			goto hasError
		}
		ret = v1
	}
	return
hasError:
	err = errors.New("Invalid " + fmt.Sprint(reflect.TypeOf(v)) + " value (" + fmt.Sprint(v) + ") for \"" + col.Name + "\" of " + col.Type.Name())
	return
}

func getTableScheme(db fdb.Transactor, dbName string, table *AstTableName) (scheme *TableScheme, err error) {
	if dbName == "" {
		dbName = table.DatabaseName()
	}
	if dbName == "" {
		err = errors.New("No database name has been specified. USE a database name, or explicitly specify databasename.tablename")
		return
	}
	scheme, err = GetTableScheme(db, dbName, table.TableName())
	return
}

func validateConditionArgs(scheme *TableScheme, origConds []condition, args []interface{}) (conds []condition, err error) {
	conds = make([]condition, len(origConds))
	copy(conds, origConds)
	for i := range conds {
		cond := &conds[i]
		col := scheme.Keys[i]
		if p, ok := cond.Equal.(placeholder); ok {
			cond.Equal, err = validateValue(col, args[int(p)])
			if err != nil {
				return
			}
		}
		if p, ok := cond.Start[0].(placeholder); ok {
			cond.Start[0], err = validateValue(col, args[int(p)])
			if err != nil {
				return
			}
		}
		if p, ok := cond.End[0].(placeholder); ok {
			cond.End[0], err = validateValue(col, args[int(p)])
			if err != nil {
				return
			}
		}
	}
	return
}

func getAdjTuples(stmt *selectStmt) (err error) {
	var adjs []adjTuple
	nbackward := 0
	nforward := 0
	for i, sfunc := range stmt.Funcs {
		if sfunc == nil {
			continue
		}
		j := 0
		switch sfunc.Name {
		case "adj_px":
			j = 1
		case "adj_vol":
			j = 2
		default:
			continue
		}
		var backward bool
		if sfunc.Params != nil && *sfunc.Params[0].Boolean {
			backward = true
			nbackward += 1
		} else {
			nforward += 1
		}
		stmt.Funcs[i] = nil
		col := stmt.Cols[i]
		if !col.IsKey {
			adjs = append(adjs, adjTuple{int(col.Pos), j, backward})
		}
	}
	stmt.Adjs = adjs
	if adjs != nil {
		if stmt.Scheme.Keys[0].Type != Int {
			err = errors.New("The first key of the table must be int for applying adj")
		}
		if stmt.Scheme.Keys[len(stmt.Scheme.Keys)-1].Type != Timestamp {
			err = errors.New("The last key of the table must be timestamp for applying adj")
		}
		if nbackward > 0 && nforward > 0 {
			err = errors.New("Mixed backward and forward adj not allowed")
		}
	}
	return
}
