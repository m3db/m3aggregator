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

// Automatically generated by MockGen. DO NOT EDIT!
// Source: github.com/m3db/m3aggregator/client/queue.go

package client

import (
	"github.com/m3db/m3metrics/encoding/protobuf"

	"github.com/golang/mock/gomock"
)

// Mock of instanceQueue interface
type MockinstanceQueue struct {
	ctrl     *gomock.Controller
	recorder *_MockinstanceQueueRecorder
}

// Recorder for MockinstanceQueue (not exported)
type _MockinstanceQueueRecorder struct {
	mock *MockinstanceQueue
}

func NewMockinstanceQueue(ctrl *gomock.Controller) *MockinstanceQueue {
	mock := &MockinstanceQueue{ctrl: ctrl}
	mock.recorder = &_MockinstanceQueueRecorder{mock}
	return mock
}

func (_m *MockinstanceQueue) EXPECT() *_MockinstanceQueueRecorder {
	return _m.recorder
}

func (_m *MockinstanceQueue) Enqueue(buf protobuf.Buffer) error {
	ret := _m.ctrl.Call(_m, "Enqueue", buf)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockinstanceQueueRecorder) Enqueue(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Enqueue", arg0)
}

func (_m *MockinstanceQueue) Close() error {
	ret := _m.ctrl.Call(_m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockinstanceQueueRecorder) Close() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Close")
}
