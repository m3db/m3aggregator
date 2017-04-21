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

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/services/m3aggregator/config"
	"github.com/m3db/m3aggregator/services/m3aggregator/serve"
	"github.com/m3db/m3x/config"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
)

const (
	gracefulShutdownTimeout = 10 * time.Second
)

var (
	configFile = flag.String("f", "", "configuration file")
	logger     = xlog.NewLevelLogger(xlog.SimpleLogger, xlog.LogLevelInfo)
)

func main() {
	flag.Parse()

	if len(*configFile) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, *configFile); err != nil {
		logger.Fatalf("error loading config file: %v", err)
	}

	// Create metrics scope.
	scope, closer, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatalf("error creating metrics root scope: %v", err)
	}
	defer closer.Close()
	instrumentOpts := instrument.NewOptions().
		SetMetricsScope(scope).
		SetMetricsSamplingRate(cfg.Metrics.SampleRate()).
		SetReportInterval(cfg.Metrics.ReportInterval())

	// Create the aggregator.
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("aggregator"))
	aggregatorOpts, err := cfg.Aggregator.NewAggregatorOptions(iOpts)
	if err != nil {
		logger.Fatalf("error creating aggregator options: %v", err)
	}
	aggregator := aggregator.NewAggregator(aggregatorOpts)

	// Create the msgpack server options.
	msgpackAddr := cfg.Msgpack.ListenAddress
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("msgpack-server"))
	msgpackServerOpts := cfg.Msgpack.NewMsgpackServerOptions(iOpts)

	// Create the http server options.
	httpAddr := cfg.HTTP.ListenAddress
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("http-server"))
	httpServerOpts := cfg.HTTP.NewHTTPServerOptions(iOpts)

	doneCh := make(chan struct{})
	closedCh := make(chan struct{})
	go func() {
		if err := serve.Serve(
			msgpackAddr,
			msgpackServerOpts,
			httpAddr,
			httpServerOpts,
			aggregator,
			doneCh,
		); err != nil {
			logger.Fatalf("could not start serving traffic: %v", err)
		}
		logger.Debug("server closed")
		close(closedCh)
	}()

	// Handle interrupts.
	logger.Warnf("interrupt: %v", interrupt())

	close(doneCh)

	select {
	case <-closedCh:
		logger.Info("server closed clean")
	case <-time.After(gracefulShutdownTimeout):
		logger.Infof("server closed due to %s timeout", gracefulShutdownTimeout.String())
	}
}

func interrupt() error {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
