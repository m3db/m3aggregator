package router

import (
	"testing"

	"github.com/m3db/m3aggregator/aggregator/handler/common"
	"github.com/m3db/m3metrics/encoding/msgpack"
	"github.com/m3db/m3msg/producer"

	"github.com/stretchr/testify/require"
)

func TestM3msgMessageDecRefBuffer(t *testing.T) {
	buf := common.NewRefCountedBuffer(msgpack.NewBufferedEncoder())
	msg := newMessage(2, buf)
	require.Equal(t, uint32(2), msg.Shard())
	require.Equal(t, uint32(0), msg.Size())
	require.Empty(t, msg.Bytes())

	msg.Finalize(producer.Consumed)
	require.Panics(t, buf.DecRef)
}
