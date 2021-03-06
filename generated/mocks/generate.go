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

// mockgen rules for generating mocks for exported interfaces (reflection mode).
//go:generate sh -c "mockgen -package=aggregator $PACKAGE/aggregator ElectionManager,FlushTimesManager,PlacementManager | mockclean -pkg $PACKAGE/aggregator -out $GOPATH/src/$PACKAGE/aggregator/aggregator_mock.go"
//go:generate sh -c "mockgen -package=client $PACKAGE/client Client,AdminClient | mockclean -pkg $PACKAGE/client -out $GOPATH/src/$PACKAGE/client/client_mock.go"
//go:generate sh -c "mockgen -package=handler $PACKAGE/aggregator/handler Handler | mockclean -pkg $PACKAGE/aggregator/handler -out $GOPATH/src/$PACKAGE/aggregator/handler/handler_mock.go"
//go:generate sh -c "mockgen -package=writer $PACKAGE/aggregator/handler/writer Writer | mockclean -pkg $PACKAGE/aggregator/handler/writer -out $GOPATH/src/$PACKAGE/aggregator/handler/writer/writer_mock.go"
//go:generate sh -c "mockgen -package=common $PACKAGE/aggregator/handler/common Queue | mockclean -pkg $PACKAGE/aggregator/handler/common -out $GOPATH/src/$PACKAGE/aggregator/handler/common/common_mock.go"
//go:generate sh -c "mockgen -package=router $PACKAGE/aggregator/handler/router Router | mockclean -pkg $PACKAGE/aggregator/handler/router -out $GOPATH/src/$PACKAGE/aggregator/handler/router/router_mock.go"
//go:generate sh -c "mockgen -package=runtime $PACKAGE/runtime OptionsWatcher | mockclean -pkg $PACKAGE/runtime -out $GOPATH/src/$PACKAGE/runtime/runtime_mock.go"

// mockgen rules for generating mocks for unexported interfaces (file mode).
//go:generate sh -c "mockgen -package=aggregator -destination=$GOPATH/src/$PACKAGE/aggregator/flush_mgr_mock.go -source=$GOPATH/src/$PACKAGE/aggregator/flush_mgr.go"
//go:generate sh -c "mockgen -package=aggregator -destination=$GOPATH/src/$PACKAGE/aggregator/flush_mock.go -source=$GOPATH/src/$PACKAGE/aggregator/flush.go"
//go:generate sh -c "mockgen -package=client -destination=$GOPATH/src/$PACKAGE/client/writer_mgr_mock.go -source=$GOPATH/src/$PACKAGE/client/writer_mgr.go"
//go:generate sh -c "mockgen -package=client -destination=$GOPATH/src/$PACKAGE/client/writer_mock.go -source=$GOPATH/src/$PACKAGE/client/writer.go"
//go:generate sh -c "mockgen -package=client -destination=$GOPATH/src/$PACKAGE/client/queue_mock.go -source=$GOPATH/src/$PACKAGE/client/queue.go"
//go:generate sh -c "mockgen -package=deploy -destination=$GOPATH/src/$PACKAGE/tools/deploy/client_mock.go -source=$GOPATH/src/$PACKAGE/tools/deploy/client.go"
//go:generate sh -c "mockgen -package=deploy -destination=$GOPATH/src/$PACKAGE/tools/deploy/manager_mock.go -source=$GOPATH/src/$PACKAGE/tools/deploy/manager.go"
//go:generate sh -c "mockgen -package=deploy -destination=$GOPATH/src/$PACKAGE/tools/deploy/planner_mock.go -source=$GOPATH/src/$PACKAGE/tools/deploy/planner.go"
//go:generate sh -c "mockgen -package=deploy -destination=$GOPATH/src/$PACKAGE/tools/deploy/validator_mock.go -source=$GOPATH/src/$PACKAGE/tools/deploy/validator.go"

package mocks
