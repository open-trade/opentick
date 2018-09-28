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
	conn.Close()
}
