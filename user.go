package opentick

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"strings"
	"sync"
)

var userMap = sync.Map{}

const (
	ReadablePerm = 0
	WritablePerm = 1
)

type User struct {
	name     string
	password string
	isAdmin  bool
	perm     map[string]int
}

func LoadUsers(db fdb.Transactor) (err error) {
	if hasMeta, _ := HasDatabase(getDB(), "_meta_"); !hasMeta {
		CreateDatabase(db, "_meta_")
	}
	_, err = Execute(db, "_meta_", "create table if not exists user(name text, password text, is_admin boolean, perm text, primary key(name)", nil)
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
		user := User{row[0].(string), row[1].(string), row[2].(bool), make(map[string]int)}
		strs := strings.Split(row[3].(string), ";")
		if len(strs) > 0 {
			for _, str := range strs {
				ab := strings.Split(str, "=")
				if len(ab) == 2 {
					perm := ReadablePerm
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
