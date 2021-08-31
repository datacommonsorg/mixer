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

package base

import (
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// PreC is the prefix for Column
	PreC = "C:"
	// PreE is the prefix for Entity
	PreE = "E:"
	// Arrow is the symbol in schema mapping between table name and table column
	Arrow = "->"
	// TypeOf represents "typeOf" literal
	TypeOf = "typeOf"
	// Triple represents Triples table name
	Triple = "Triple"
	// Triple represents Triples table name
	DCS = "dcs:"
)

func toBqTable(table string, db string) string {
	return fmt.Sprintf("`%s.%s`", db, table)
}

// QueryOptions contains options for query.
type QueryOptions struct {
	Limit    int
	Db       string
	Prov     bool
	Distinct bool
	Orderby  string
	ASC      bool
}

// Node represents a reference of a graph node in datalog query.
type Node struct {
	// Alias of a node in datalog query, should start with "?".
	Alias string
}

func (n Node) String() string {
	return n.Alias
}

// NewNode creates a new Node instance.
func NewNode(nodeAlias string) Node {
	return Node{nodeAlias}
}

// Table represents a spanner table in through translation process
type Table struct {
	// Spanner table name.
	Name string
	// Id used to distinguish same table when self join.
	ID string
}

func (t Table) String() string {
	return fmt.Sprintf("%s%s", t.Name, t.ID)
}

// Alias gets table's alias used in SQL query.
func (t Table) Alias() string {
	r, _ := regexp.Compile("[.`:-]")
	return r.ReplaceAllString(t.Name, "_") + t.ID
}

// Entity represents node entity referenced in schema mapping.
type Entity struct {
	// Entity ID like "E1", "E2"
	ID string
	// The spanner table this entity belongs to.
	Table Table
}

// NewEntity creates a new Entity instance.
func NewEntity(s string, db string) (*Entity, error) {
	if !strings.HasPrefix(s, PreE) {
		return nil, status.Error(codes.InvalidArgument, "Invalid input for Entity")
	}
	parts := strings.SplitN(strings.TrimPrefix(s, PreE), Arrow, 2)
	if len(parts) != 2 {
		return nil, status.Error(codes.InvalidArgument, "Invalid input for Entity")
	}
	return &Entity{ID: parts[1], Table: Table{Name: toBqTable(parts[0], db)}}, nil
}

func (e Entity) String() string {
	return fmt.Sprintf("%s%s->%s", e.Table.Name, e.Table.ID, e.ID)
}

// Key gets the key of an entity.
func (e Entity) Key() string {
	return strings.Join([]string{e.Table.Name, e.ID}, Arrow)
}

// Column represents a column in schema mapping.
type Column struct {
	// Spanner column name.
	Name string
	// The spanner table this column belongs to.
	Table Table
}

func (c Column) String() string {
	return fmt.Sprintf("%s%s->%s", c.Table.Name, c.Table.ID, c.Name)
}

// Key gets the key of a column.
func (c Column) Key() string {
	return strings.Join([]string{c.Table.Name, c.Name}, Arrow)
}

// NewColumn creates a new Column instance.
func NewColumn(s string, db string) (*Column, error) {
	if !strings.HasPrefix(s, PreC) {
		return nil, status.Error(codes.InvalidArgument, "Invalid input for Column")
	}
	parts := strings.SplitN(strings.TrimPrefix(s, PreC), Arrow, 2)
	if len(parts) != 2 {
		return nil, status.Error(codes.InvalidArgument, "Invalid input for Column")
	}
	return &Column{Name: parts[1], Table: Table{Name: toBqTable(parts[0], db)}}, nil
}

// FuncDeps represents the functional deps predicate.
type FuncDeps struct {
}

// Mapping represents a schema mapping statement.
type Mapping struct {
	// Predicate could be a string value or an Entity.
	Pred interface{}
	// Subject is a table entity.
	Sub Entity
	// Object indicates the column of the entity.
	Obj interface{}
}

// NewMapping creates a new Mapping instance.
func NewMapping(pred, sub, obj, db string) (*Mapping, error) {
	var p interface{}
	var o interface{}
	var err error

	s, err := NewEntity(sub, db)
	if err != nil {
		return nil, err
	}

	if pred == "functionalDeps" {
		p = FuncDeps{}
	} else if strings.HasPrefix(pred, PreC) {
		p, err = NewColumn(pred, db)
		if err != nil {
			return nil, err
		}
		p = *p.(*Column)
	} else {
		p = pred
	}

	if pred == "functionalDeps" {
		objList := strings.Split(obj, ",")
		for i := range objList {
			objList[i] = strings.TrimSpace(objList[i])
		}
		o = objList
	} else {
		if strings.HasPrefix(obj, PreC) {
			o, err = NewColumn(obj, db)
			if err != nil {
				return nil, err
			}
			o = *o.(*Column)
		} else if strings.HasPrefix(obj, PreE) {
			o, err = NewEntity(obj, db)
			o = *o.(*Entity)
		} else {
			o = obj
		}
		if err != nil {
			return nil, err
		}
	}

	return &Mapping{Pred: p, Sub: *s, Obj: o}, nil
}

// IsTriple checks if a mapping is about Triples table.
func (q *Mapping) IsTriple() bool {
	return strings.Contains(q.Sub.Table.Name, Triple)
}

// Query represents a datalog query statement.
type Query struct {
	// Query predicate is a string of schema.
	Pred string
	// Query subject is a node.
	Sub Node
	// Query object is a node or string.
	Obj interface{}
}

// NewQuery creates a new Query instance.
func NewQuery(pred string, nodeAlias string, obj interface{}) *Query {
	return &Query{Pred: pred, Sub: NewNode(nodeAlias), Obj: obj}
}

// IsTypeOf checks if a query is typeOf statement.
func (q *Query) IsTypeOf() bool {
	return q.Pred == TypeOf
}
