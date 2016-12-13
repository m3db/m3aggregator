// Copyright (c) 2016 Uber Technologies, Inc.
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

package msgpack

import (
	"errors"

	"github.com/m3db/m3aggregator/pool"
)

var (
	errNoBufferedEncoderPool = errors.New("no buffered encoder pool")
	errNoFloatsPool          = errors.New("no floats pool")
	errNoPoliciesPool        = errors.New("no policies pool")
)

type encoderOptions struct {
	encoderPool BufferedEncoderPool
}

// NewEncoderOptions creates a new set of encoder options
func NewEncoderOptions() EncoderOptions {
	encoderPool := NewBufferedEncoderPool(nil)
	encoderPool.Init(newBufferedEncoder)

	return encoderOptions{
		encoderPool: encoderPool,
	}
}

func (o encoderOptions) SetBufferedEncoderPool(value BufferedEncoderPool) EncoderOptions {
	opts := o
	opts.encoderPool = value
	return opts
}

func (o encoderOptions) BufferedEncoderPool() BufferedEncoderPool {
	return o.encoderPool
}

func (o encoderOptions) Validate() error {
	if o.encoderPool == nil {
		return errNoBufferedEncoderPool
	}
	return nil
}

type iteratorOptions struct {
	floatsPool   pool.FloatsPool
	policiesPool pool.PoliciesPool
}

// NewIteratorOptions creates a new set of iterator options
func NewIteratorOptions() IteratorOptions {
	floatsPool := pool.NewFloatsPool(nil, nil)
	floatsPool.Init()

	policiesPool := pool.NewPoliciesPool(nil, nil)
	policiesPool.Init()

	return iteratorOptions{
		floatsPool:   floatsPool,
		policiesPool: policiesPool,
	}
}

func (o iteratorOptions) SetFloatsPool(value pool.FloatsPool) IteratorOptions {
	opts := o
	opts.floatsPool = value
	return opts
}

func (o iteratorOptions) FloatsPool() pool.FloatsPool {
	return o.floatsPool
}

func (o iteratorOptions) SetPoliciesPool(value pool.PoliciesPool) IteratorOptions {
	opts := o
	opts.policiesPool = value
	return opts
}

func (o iteratorOptions) PoliciesPool() pool.PoliciesPool {
	return o.policiesPool
}

func (o iteratorOptions) Validate() error {
	if o.floatsPool == nil {
		return errNoFloatsPool
	}
	if o.policiesPool == nil {
		return errNoPoliciesPool
	}
	return nil
}
