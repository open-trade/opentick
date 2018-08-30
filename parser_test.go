package opentick

import (
	"github.com/alecthomas/repr"
	"testing"
)

var sqlStmt = "select * from test where a > 1.2 and (b < 2 - 1) and b in (1,2) limit -2"

func Test_Parse(t *testing.T) {
	expr, err := Parse(sqlStmt)
	repr.Println(expr, repr.Indent("  "), repr.OmitEmpty(true))
	if err != nil {
		t.Error(err)
	}
}

func Benchmark_Parse(b *testing.B) {
	b.StopTimer() //调用该函数停止压力测试的时间计数

	//做一些初始化的工作,例如读取文件数据,数据库连接之类的,
	//这样这些时间不影响我们测试函数本身的性能

	b.StartTimer()             //重新开始时间
	for i := 0; i < b.N; i++ { //use b.N for looping
		_, err := Parse(sqlStmt)
		if err != nil {
			b.Fatal(err)
		}
	}
}
