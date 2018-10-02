package main

import (
	"flag"
	"github.com/opentradesolutions/opentick"
	// "github.com/pkg/profile"
)

var addr = flag.String("addr", "0.0.0.0:1116", "tcp listen address")

func main() {
	// CPU profiling by default
	// defer profile.Start().Stop()
	// defer profile.Start(profile.MemProfile).Stop()
	// go tool pprof --pdf ~/go/bin/yourbinary /var/path/to/cpu.pprof > file.pdf
	flag.Parse()
	opentick.StartServer(*addr)
}
