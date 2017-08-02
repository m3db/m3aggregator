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

package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3x/errors"
)

// A list of HTTP endpoints.
const (
	HealthPath = "/health"
	ResignPath = "/resign"
	StatusPath = "/status"
)

var (
	errRequestMustBeGet  = xerrors.NewInvalidParamsError(errors.New("request must be GET"))
	errRequestMustBePost = xerrors.NewInvalidParamsError(errors.New("request must be POST"))
)

func registerHandlers(mux *http.ServeMux, aggregator aggregator.Aggregator) {
	registerHealthHandler(mux)
	registerResignHandler(mux, aggregator)
	registerStatusHandler(mux, aggregator)
}

func registerHealthHandler(mux *http.ServeMux) {
	mux.HandleFunc(HealthPath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if httpMethod := strings.ToUpper(r.Method); httpMethod != http.MethodGet {
			writeErrorResponse(w, errRequestMustBeGet)
			return
		}
		writeSuccessResponse(w, nil)
	})
}

func registerResignHandler(mux *http.ServeMux, aggregator aggregator.Aggregator) {
	mux.HandleFunc(ResignPath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if httpMethod := strings.ToUpper(r.Method); httpMethod != http.MethodPost {
			writeErrorResponse(w, errRequestMustBePost)
			return
		}

		if err := aggregator.Resign(); err != nil {
			writeErrorResponse(w, err)
			return
		}
		writeSuccessResponse(w, nil)
	})
}

func registerStatusHandler(mux *http.ServeMux, aggregator aggregator.Aggregator) {
	mux.HandleFunc(StatusPath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if httpMethod := strings.ToUpper(r.Method); httpMethod != http.MethodGet {
			writeErrorResponse(w, errRequestMustBeGet)
			return
		}

		status := aggregator.Status()
		writeSuccessResponse(w, status)
	})
}

// Response is an HTTP response.
type Response struct {
	Status string      `json:"status,omitempty"`
	Error  string      `json:"error,omitempty"`
	Data   interface{} `json:"data,omitempty"`
}

// NewResponse creates a new empty response.
func NewResponse() Response { return Response{} }

// NewStatusResponse creates a new empty status response.
func NewStatusResponse() Response { return Response{Data: aggregator.RuntimeStatus{}} }

func newSuccessResponse(data interface{}) Response {
	return Response{Status: "OK", Data: data}
}

func newErrorResponse(err error, data interface{}) Response {
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	return Response{Status: "Error", Error: errStr, Data: data}
}

func writeSuccessResponse(w http.ResponseWriter, data interface{}) {
	writeResponse(w, data, nil)
}

func writeErrorResponse(w http.ResponseWriter, err error) {
	writeResponse(w, nil, err)
}

func writeResponse(w http.ResponseWriter, data interface{}, err error) {
	resp := newSuccessResponse(data)
	if err != nil {
		resp = newErrorResponse(err, data)
	}

	buf := bytes.NewBuffer(nil)
	if encodeErr := json.NewEncoder(buf).Encode(&resp); encodeErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		resp = newErrorResponse(encodeErr, data)
		json.NewEncoder(w).Encode(&resp)
		return
	}

	if err == nil {
		w.WriteHeader(http.StatusOK)
	} else if xerrors.IsInvalidParams(err) {
		w.WriteHeader(http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(buf.Bytes())
}
