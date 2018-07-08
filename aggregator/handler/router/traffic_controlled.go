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
	"github.com/m3db/m3aggregator/aggregator/handler/common"

	"github.com/uber-go/tally"
)

type trafficControlledRouterMetrics struct {
	trafficControlNotAllwed tally.Counter
}

func newTrafficControlledRouterMetrics(scope tally.Scope) trafficControlledRouterMetrics {
	return trafficControlledRouterMetrics{
		trafficControlNotAllwed: scope.Counter("traffic-control-not-allowed"),
	}
}

type trafficControlledRouter struct {
	common.TrafficController
	Router

	m trafficControlledRouterMetrics
}

// NewTrafficControlledRouter creates a traffic controlled router.
func NewTrafficControlledRouter(
	trafficController common.TrafficController,
	router Router,
	scope tally.Scope,
) Router {
	return &trafficControlledRouter{
		TrafficController: trafficController,
		Router:            router,
		m:                 newTrafficControlledRouterMetrics(scope),
	}
}

func (r *trafficControlledRouter) Route(shard uint32, buffer *common.RefCountedBuffer) error {
	if !r.TrafficController.Allow() {
		buffer.DecRef()
		r.m.trafficControlNotAllwed.Inc(1)
		return nil
	}
	return r.Router.Route(shard, buffer)
}

func (r *trafficControlledRouter) Close() {
	r.TrafficController.Close()
	r.Router.Close()
}
