package main

import (
	"flag"
	"github.com/opentradesolutions/opentick"
	// "github.com/pkg/profile"
)

var addr = flag.String("addr", "0.0.0.0:1116", "tcp listen address")
var fdbClusterFile = flag.String("fdb_cluster_file", "", "path of fdb cluster file, use default path if not specified")
var n1 = flag.Int("num_foundation_db_connections", 1, "number of connections to underlying FoundationDB")
var n2 = flag.Int("max_concurrency", 100, "max concurrency of one connection, too big concurrency may cause performance degradation")
var n3 = flag.Int("timeout", 30, "client connection timeout in seconds, heartbeat applied")
var n4 = flag.Float64("cache", 0, "cache expiration time in seconds, 0 means no cache")

func main() {
	// CPU profiling by default
	// defer profile.Start().Stop()
	// defer profile.Start(profile.MemProfile).Stop()
	// go tool pprof --pdf ~/go/bin/yourbinary /var/path/to/cpu.pprof > file.pdf
	flag.Parse()
	err := opentick.StartServer(*addr, *fdbClusterFile, *n1, *n2, *n3, *n4)
	if err != nil {
		panic(err)
	}
}
