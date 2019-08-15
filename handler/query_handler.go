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
	"context"
	"encoding/json"
	"net/http"

	"github.com/datacommonsorg/mixer/models"
	"github.com/datacommonsorg/mixer/store"
	"github.com/gorilla/schema"
)

// QueryPostHandler handles POST request to /query
func QueryPostHandler(st store.Interface, w http.ResponseWriter, r *http.Request) {
	var req models.QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if _, ok := err.(*json.SyntaxError); ok {
			errorf(w, http.StatusBadRequest, "Body was not valid JSON: %v", err)
			return
		}
		errorf(w, http.StatusInternalServerError, "Could not get body: %v", err)
		return
	}

	var resp models.QueryResponse
	ctx := context.Background()
	if err := st.Query(ctx, &req, &resp); err != nil {
		errorf(w, http.StatusInternalServerError, "Query get error: %v", err)
		return
	}
	b, err := resp.MarshalBinary()
	if err != nil {
		errorf(w, http.StatusInternalServerError, "Could not marshal JSON: %v", err)
		return
	}
	w.Write(b)
}

// QueryGetHandler handles GET request to /query
func QueryGetHandler(st store.Interface, w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		errorf(w, http.StatusBadRequest, "Could not parse request url: %v", err)
		return
	}

	req := new(models.QueryRequest)
	if err := schema.NewDecoder().Decode(req, r.Form); err != nil {
		errorf(w, http.StatusBadRequest, "Request url has invalid parameters: %v", err)
		return
	}

	var resp models.QueryResponse
	ctx := context.Background()
	if err := st.Query(ctx, req, &resp); err != nil {
		errorf(w, http.StatusInternalServerError, "Query get error: %v", err)
		return
	}
	b, err := resp.MarshalBinary()
	if err != nil {
		errorf(w, http.StatusInternalServerError, "Could not marshal JSON: %v", err)
		return
	}
	w.Write(b)
}
