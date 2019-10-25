package opentick

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"strings"
	"sync"
)

var userMap = sync.Map{}

type PermType int

const (
	NoPerm       PermType = iota
	ReadablePerm          = 1
	WritablePerm          = 2
)

type User struct {
	name     string
	password string
	isAdmin  bool
	perm     map[string]PermType
}

func LoadUsers(db fdb.Transactor) (err error) {
	if hasMeta, _ := HasDatabase(db, "_meta_"); !hasMeta {
		CreateDatabase(db, "_meta_")
	}
	_, err = Execute(db, "_meta_", "create table if not exists user(name text, password text, is_admin boolean, perm text, primary key(name))", nil)
	if err != nil {
		return
	}
	var res [][]interface{}
	res, err = Execute(db, "_meta_", "select * from user", nil)
	userMap.Range(func(key interface{}, value interface{}) bool {
		userMap.Delete(key)
		return true
	})
	for _, row := range res {
		user := &User{row[0].(string), row[1].(string), row[2].(bool), make(map[string]PermType)}
		strs := strings.Split(row[3].(string), ";")
		if len(strs) > 0 {
			for _, str := range strs {
				ab := strings.Split(str, "=")
				if len(ab) == 2 {
					var perm PermType
					perm = ReadablePerm
					if ab[1] == "write" {
						perm = WritablePerm
					}
					user.perm[ab[0]] = perm
				}
			}
		}
		userMap.Store(user.name, user)
	}
	return
}

func GetPerm(dbName string, tblName string, users ...*User) PermType {
	if len(users) == 0 {
		return WritablePerm
	}
	user := users[0]
	if user.isAdmin {
		return WritablePerm
	}
	perm1, _ := user.perm[dbName]
	if perm1 == WritablePerm {
		return perm1
	}
	if tblName == "" {
		return perm1
	}
	perm2, _ := user.perm[dbName+"."+tblName]
	if perm2 > perm1 {
		return perm2
	}
	return perm1
}
