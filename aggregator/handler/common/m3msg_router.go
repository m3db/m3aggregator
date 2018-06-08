// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"time"

	"github.com/m3db/m3msg/producer"
)

const (
	defaultCloseTimeout = 5 * time.Second
)

type m3msgRouter struct {
	p            producer.Producer
	closeTimeout time.Duration
}

// NewM3msgRouter creates a new router that routes all data through m3msg producer.
func NewM3msgRouter(p producer.Producer, closeTimeout *time.Duration) Router {
	timeout := defaultCloseTimeout
	if closeTimeout != nil && *closeTimeout > 0 {
		timeout = *closeTimeout
	}
	return m3msgRouter{p: p, closeTimeout: timeout}
}

func (r m3msgRouter) Route(shard uint32, buffer *RefCountedBuffer) error {
	return r.p.Produce(newMessage(shard, buffer))
}

func (r m3msgRouter) Close() {
	doneCh := make(chan struct{})

	go func() {
		r.p.Close(producer.WaitForConsumption)
		close(doneCh)
	}()
	select {
	case <-time.After(r.closeTimeout):
	case <-doneCh:
	}
}

type message struct {
	shard  uint32
	buffer *RefCountedBuffer
}

func newMessage(shard uint32, buffer *RefCountedBuffer) message {
	return message{shard: shard, buffer: buffer}
}

func (d message) Shard() uint32 {
	return d.shard
}

func (d message) Bytes() []byte {
	return d.buffer.Buffer().Bytes()
}

func (d message) Size() uint32 {
	return uint32(len(d.buffer.Buffer().Bytes()))
}

func (d message) Finalize(producer.FinalizeReason) {
	d.buffer.DecRef()
}
