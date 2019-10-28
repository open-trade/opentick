package opentick

import (
	"crypto/sha1"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"io"
	"strings"
	"sync"
)

var userMap = sync.Map{}

type PermType int

const (
	NoPerm PermType = iota
	ReadablePerm
	WritablePerm
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
		user := &User{name: row[0].(string), password: row[1].(string), isAdmin: row[2].(bool), perm: make(map[string]PermType)}
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

func (user *User) Perm2Str() (res string) {
	if user.perm == nil {
		return
	}
	var perms []string
	for k, v := range user.perm {
		tmp := k + "="
		if v == WritablePerm {
			tmp += "write"
		} else {
			tmp += "read"
		}
		perms = append(perms, tmp)
	}
	return strings.Join(perms, ";")
}

func (user *User) CheckPassword(password string) bool {
	return user.password == sha1String(password)
}

func (user User) UpdatePasswd(db fdb.Transactor, newpasswd string) error {
	_, err := Execute(db, "_meta_", "insert into user values(?, ?, ?, ?)", []interface{}{user.name, newpasswd, user.isAdmin, user.Perm2Str()})
	if err != nil {
		return err
	}
	user.password = sha1String(newpasswd)
	userMap.Store(user.name, &user)
	return nil
}

func sha1String(password string) string {
	h := sha1.New()
	io.WriteString(h, password)
	return fmt.Sprintf("%x", h.Sum(nil))
}
