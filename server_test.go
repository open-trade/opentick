package opentick

import (
	"github.com/opentradesolutions/opentick/client"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_Server(t *testing.T) {
	port, _ := freeport.GetFreePort()
	go StartServer("", port)
	time.Sleep(100 * time.Millisecond)
	conn, err := client.Connect("", port, "test")
	assert.Equal(t, nil, err)
	var ret [][]interface{}
	var fut client.Future
	fut, err = conn.Execute("select * from test where a=1")
	ret, err = fut.Get()
	assert.Equal(t, nil, ret)
	assert.Equal(t, nil, err)
	defer conn.Close()
}
