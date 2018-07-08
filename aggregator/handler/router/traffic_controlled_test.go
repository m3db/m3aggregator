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

package router

import (
	"testing"

	"github.com/m3db/m3aggregator/aggregator/handler/common"
	"github.com/m3db/m3metrics/encoding/msgpack"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestTrafficControlledRouter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := NewMockRouter(ctrl)
	r := NewTrafficControlledRouter(common.NewTrafficDisabler(
		common.NewTrafficControlOptions()),
		m,
		tally.NoopScope,
	)
	buf1 := common.NewRefCountedBuffer(msgpack.NewPooledBufferedEncoderSize(nil, 1024))
	m.EXPECT().Route(uint32(1), buf1)
	require.NoError(t, r.Route(1, buf1))

	buf2 := common.NewRefCountedBuffer(msgpack.NewPooledBufferedEncoderSize(nil, 1024))
	r = NewTrafficControlledRouter(common.NewTrafficEnabler(
		common.NewTrafficControlOptions()),
		m,
		tally.NoopScope,
	)
	require.NoError(t, r.Route(2, buf2))
	require.Panics(t, buf2.DecRef)
	m.EXPECT().Close()
	r.Close()
}
