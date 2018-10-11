package main

import (
	"flag"
	"github.com/opentradesolutions/opentick"
	// "github.com/pkg/profile"
)

var addr = flag.String("addr", "0.0.0.0:1116", "tcp listen address")
var fdbClusterFile = flag.String("fdb_cluster_file", "", "path of fdb cluster file, use default path if not specified")
var n = flag.Int("num_foundation_db_connections", 1, "number of connections to underlying FoundationDB")
var n2 = flag.Int("max_concurrency", 100, "max concurrency of one connection, too big concurrency may cause performance degradation")

func main() {
	// CPU profiling by default
	// defer profile.Start().Stop()
	// defer profile.Start(profile.MemProfile).Stop()
	// go tool pprof --pdf ~/go/bin/yourbinary /var/path/to/cpu.pprof > file.pdf
	flag.Parse()
	err := opentick.StartServer(*addr, *fdbClusterFile, *n, *n2)
	if err != nil {
		panic(err)
	}
}
