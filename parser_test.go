package opentick_test

import (
	"github.com/alecthomas/repr"
	"github.com/opentradesolutions/opentick"
	"testing"
)

func Test_Parse(t *testing.T) {
	expr, err := opentick.Parse("select * from test")
	repr.Println(expr, repr.Indent("  "), repr.OmitEmpty(true))
	if err != nil {
		t.Error("invalid select")
	}
}

func Benchmark_Parse(b *testing.B) {
	b.StopTimer() //调用该函数停止压力测试的时间计数

	//做一些初始化的工作,例如读取文件数据,数据库连接之类的,
	//这样这些时间不影响我们测试函数本身的性能

	b.StartTimer()             //重新开始时间
	for i := 0; i < b.N; i++ { //use b.N for looping
		opentick.Parse("select * from test")
	}
}
