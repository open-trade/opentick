package main

import (
	"flag"
	"github.com/opentradesolutions/opentick"
)

var addr = flag.String("addr", "0.0.0.0:1116", "tcp listen address")

func main() {
	flag.Parse()
	opentick.StartServer(*addr)
}
