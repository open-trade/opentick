package opentick

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"sync"
)

type adjValue struct {
	Tm   int64
	Px   float64
	Vol  float64
	PxB  float64
	VolB float64
}

func (adj adjValue) get(b bool, t int) float64 {
	if t == 1 {
		if b {
			return adj.PxB
		}
		return adj.Px
	} else if t == 2 {
		if b {
			return adj.VolB
		}
		return adj.Vol
	}
	return 1.
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
				ret = append(ret, adjValue{tm, px, vol, px, vol})
			}
		}
		n := len(ret)
		if n > 1 {
			for i := n - 2; i >= 0; i -= 1 {
				ret[i].Px *= ret[i+1].Px
				ret[i].Vol *= ret[i+1].Vol
			}
			for i := 0; i < n; i += 1 {
				ret[i].PxB = 1. / ret[i].PxB
				ret[i].VolB = 1. / ret[i].VolB
				if i > 0 {
					ret[i].PxB *= ret[i-1].PxB
					ret[i].VolB *= ret[i-1].VolB
				}
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
		b := adjs[0].Backward
		var lastSec int64
		var lastTm int64
		var adjs adjValues
		var iAdj int
		n := len(recs)
		for i := 0; i < n; i += 1 {
			j := i
			if stmt.Reverse {
				j = n - i - 1
			}
			rec := recs[j]
			key := rec[0]
			value := rec[1]
			sec, _ := getInt(key[0])
			iTm := len(key) - 1
			tm, _ := getInt(key[iTm].(tuple.Tuple)[0])
			reinit := i == 0 || sec != lastSec || tm < lastTm
			lastSec = sec
			lastTm = tm
			if reinit {
				adjs = adjCache.get(db, stmt.Schema.DbName, int(sec))
				if len(adjs) > 0 {
					iAdj = adjs.bisectRight(tm)
				}
			}
			if len(adjs) == 0 {
				continue
			}
			if !reinit {
				for iAdj < len(adjs) {
					if adjs[iAdj].Tm <= tm {
						iAdj += 1
					} else {
						break
					}
				}
			}
			k := iAdj
			if b {
				if k == 0 {
					continue
				}
				k -= 1
			} else if k == len(adjs) {
				continue
			}
			adj := adjs[k]
			for _, col := range stmt.Adjs {
				if col.Pos < len(value) {
					if v, ok := getFloat(value[col.Pos]); ok {
						value[col.Pos] = v * adj.get(b, col.Adj)
					}
				}
			}
		}
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
	adjs := adjCache.get(db, stmt.Schema.DbName, sec)
	if len(adjs) == 0 {
		return
	}
	i := adjs.bisectRight(tm)
	b := stmt.Adjs[0].Backward
	if b {
		if i == 0 {
			return
		}
		i -= 1
	} else {
		if i == len(adjs) {
			return
		}
	}
	adj := adjs[i]
	for _, col := range stmt.Adjs {
		if col.Pos < len(value) {
			if v, ok := getFloat(value[col.Pos]); ok {
				value[col.Pos] = v * adj.get(b, col.Adj)
			}
		}
	}
}
