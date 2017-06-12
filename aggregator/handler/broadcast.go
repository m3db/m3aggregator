// Copyright (c) 2017 Uber Technologies, Inc.
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

package handler

import (
	"bytes"
	"sync/atomic"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/protocol/msgpack"
)

type broadcastHandler struct {
	handlers    []aggregator.Handler
	handlersLen int32
}

// NewBroadcastHandler creates a new Handler that routes a
// msgpack buffer to a list of handlers.
func NewBroadcastHandler(handlers []aggregator.Handler) aggregator.Handler {
	return &broadcastHandler{handlers: handlers, handlersLen: int32(len(handlers))}
}

func (b *broadcastHandler) Handle(buffer msgpack.Buffer) error {
	var (
		buf = newReferencedBuffer(buffer)
		err error
	)
	for _, h := range b.handlers {
		buf.incRef()
		if err = h.Handle(buf); err != nil {
			break
		}
	}
	buf.Close()
	return err
}

func (b *broadcastHandler) Close() {
	for _, h := range b.handlers {
		h.Close()
	}
}

type referencedBuffer struct {
	// Could not embed msgpack.Buffer here due to naming conflict with method Buffer().
	buffer msgpack.Buffer
	ref    uint32
}

func newReferencedBuffer(buffer msgpack.Buffer) *referencedBuffer {
	return &referencedBuffer{buffer: buffer, ref: 1}
}

func (b *referencedBuffer) Buffer() *bytes.Buffer {
	return b.buffer.Buffer()
}

func (b *referencedBuffer) Bytes() []byte {
	return b.buffer.Bytes()
}

func (b *referencedBuffer) Reset() {
	b.buffer.Reset()
}

func (b *referencedBuffer) Close() {
	// Decrement b.ref
	ref := atomic.AddUint32(&b.ref, ^uint32(0))
	if ref > 0 {
		return
	}

	b.buffer.Close()
}

func (b *referencedBuffer) incRef() {
	atomic.AddUint32(&b.ref, 1)
}
