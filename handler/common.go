// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/datacommonsorg/mixer/store"
)

// corsHandler wraps a HTTP handler and applies the appropriate responses for Cross-Origin Resource Sharing.
type corsHandler http.HandlerFunc

func (h corsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Headers", "Authorization")
		return
	}
	h(w, r)
}

// errorf writes a swagger-compliant error response.
func errorf(w http.ResponseWriter, code int, format string, a ...interface{}) {
	var out struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	out.Code = code
	out.Message = fmt.Sprintf(format, a...)
	b, err := json.Marshal(out)
	if err != nil {
		http.Error(w, `{"code": 500, "message": "Could not format JSON for original message."}`, 500)
		return
	}
	http.Error(w, string(b), code)
}

// AppHandler wraps the http handler and app context.
type AppHandler struct {
	Handler func(st store.Interface, w http.ResponseWriter, r *http.Request)
	Store   store.Interface
}

// ServeHTTP allows your type to satisfy the http.Handler interface.
func (ah AppHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ah.Handler(ah.Store, w, r)
}
