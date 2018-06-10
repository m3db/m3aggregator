package router

import (
	"github.com/m3db/m3aggregator/aggregator/handler/common"
	"github.com/m3db/m3msg/producer"
)

type withAckRouter struct {
	p producer.Producer
}

// NewWithAckRouter creates a new router that routes buffer and waits for acknowledgements.
func NewWithAckRouter(p producer.Producer) Router {
	return withAckRouter{p: p}
}

func (r withAckRouter) Route(shard uint32, buffer *common.RefCountedBuffer) error {
	return r.p.Produce(newMessage(shard, buffer))
}

func (r withAckRouter) Close() {
	r.p.Close(producer.WaitForConsumption)
}

type message struct {
	shard  uint32
	buffer *common.RefCountedBuffer
}

func newMessage(shard uint32, buffer *common.RefCountedBuffer) producer.Message {
	return message{shard: shard, buffer: buffer}
}

func (d message) Shard() uint32 {
	return d.shard
}

func (d message) Bytes() []byte {
	return d.buffer.Buffer().Bytes()
}

func (d message) Size() uint32 {
	return uint32(len(d.Bytes()))
}

func (d message) Finalize(producer.FinalizeReason) {
	d.buffer.DecRef()
}
