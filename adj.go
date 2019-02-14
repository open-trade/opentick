package opentick

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"sync"
)

type adjValue struct {
	Tm  int64
	Px  float64
	Vol float64
}

type adjValues []adjValue

type adjCacheS struct {
	mut    sync.Mutex
	values map[string]map[int]adjValues
}

func (self *adjCacheS) clear(dbName string) {
	self.mut.Lock()
	delete(self.values, dbName)
	self.mut.Unlock()
}

var adjCache = adjCacheS{
	values: make(map[string]map[int]adjValues),
}

func (a adjValues) bisectRight(tm int64) int {
	/*
			Return the index where to insert item x in list a, assuming a is sorted.
		  The return value i is such that all e in a[:i] have e <= x, and all e in
		  a[i:] have e > x.  So if x already appears in the list, a.insert(x) will
		  insert just after the rightmost x already there.
	*/
	lo := 0
	hi := len(a)
	var mid int
	for lo < hi {
		mid = (lo + hi) / 2
		if tm < a[mid].Tm {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

var adjSelect, _ = Parse("select * from _adj_ where sec=?")

func (self *adjCacheS) get(db fdb.Transactor, dbName string, sec int) (ret adjValues) {
	self.mut.Lock()
	values, ok := self.values[dbName]
	if ok {
		if ret, ok = values[sec]; ok {
			self.mut.Unlock()
			return
		}
	} else {
		values = make(map[int]adjValues)
		self.values[dbName] = values
	}
	self.mut.Unlock()
	stmt, err := Resolve(db, dbName, adjSelect)
	if err == nil {
		tmp, err2 := ExecuteStmt(db, stmt, []interface{}{sec})
		if err2 == nil {
			for _, row := range tmp {
				if len(row) != 4 {
					break
				}
				if _, ok1 := getInt(row[0]); !ok1 {
					break
				}
				tmTuple, ok2 := row[1].(tuple.Tuple)
				if !ok2 {
					break
				}
				if len(tmTuple) != 2 {
					break
				}
				tm, ok2_1 := getInt(tmTuple[0])
				if !ok2_1 {
					break
				}
				px, ok3 := getFloat(row[2])
				if !ok3 {
					break
				}
				vol, ok4 := getFloat(row[3])
				if !ok4 {
					break
				}
				if px == 0. {
					px = 1.
				}
				if vol == 0. {
					vol = 1.
				}
				ret = append(ret, adjValue{tm, px, vol})
			}
		}
		if len(ret) > 1 {
			for i := len(ret) - 2; i >= 0; i -= 1 {
				ret[i].Px *= ret[i+1].Px
				ret[i].Vol *= ret[i+1].Vol
			}
		}
	}
	self.mut.Lock()
	values[sec] = ret
	self.mut.Unlock()
	return
}

func applyFunc(db fdb.Transactor, stmt *selectStmt, recs []([2]tuple.Tuple)) {
	adjs := stmt.Adjs
	if adjs != nil {
	}
}

func applyFuncOne(db fdb.Transactor, stmt *selectStmt, value tuple.Tuple) {
	adjs := stmt.Adjs
	if adjs != nil {
		sec, _ := getInt(stmt.Conds[0].Equal)
		tm, _ := getInt(stmt.Conds[len(stmt.Conds)-1].Equal.(tuple.Tuple)[0])
		applyAdjOne(db, stmt, int(sec), tm, value)
	}
}

func applyAdjOne(db fdb.Transactor, stmt *selectStmt, sec int, tm int64, value tuple.Tuple) {
	adjs := adjCache.get(db, stmt.Scheme.DbName, sec)
	if len(adjs) == 0 {
		return
	}
	i := adjs.bisectRight(tm)
	if i == len(adjs) {
		return
	}
	adj := adjs[i]
	for _, col := range stmt.Adjs {
		if col.Pos < len(value) {
			if v, ok := getFloat(value[col.Pos]); ok {
				if col.Adj == 1 {
					value[col.Pos] = v * adj.Px
				} else if col.Adj == 2 {
					value[col.Pos] = v * adj.Vol
				}
			}
		}
	}
}
