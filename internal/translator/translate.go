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

package translator

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/datacommonsorg/mixer/internal/parser/tmcf"
	"github.com/datacommonsorg/mixer/internal/translator/solver"
	"github.com/datacommonsorg/mixer/internal/translator/types"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const svProp = "variable_measured"

// Binding contains a query and mapping object which bind together.
type Binding struct {
	Query   *types.Query
	Mapping *types.Mapping
}

// Constraint wraps the SQL lhs and rhs variable.
type Constraint struct {
	// Left hand side of an SQL condition
	LHS types.Column
	// Right hand side of an SQL condition
	RHS interface{}
}

// entityInfo contains the information for resolved entity.
type entityInfo struct {
	e types.Entity
	c types.Column
	v interface{}
}

// Translation contains the translated result.
type Translation struct {
	SQL        string
	Nodes      []types.Node
	Bindings   []Binding
	Constraint []Constraint
	Prov       map[int][]int
}

// ProvInfo contains the provenance query metadata
type ProvInfo struct {
	query     bool
	tableProv map[string]types.Column
}

// Graph represents the struct for terms matching.
type Graph map[interface{}]map[interface{}]struct{}

func addQuote(s string, useQuote ...bool) string {
	if len(useQuote) == 0 || !useQuote[0] {
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			return s
		}
	}

	if !strings.HasPrefix(s, `"`) {
		s = `"` + s
	}
	if !strings.HasSuffix(s, `"`) {
		s += `"`
	}
	return s
}

func sortMapSet(m map[interface{}]struct{}) []interface{} {
	sorted := []interface{}{}
	for v := range m {
		sorted = append(sorted, v)
	}
	sort.SliceStable(sorted, func(i, j int) bool {
		return strings.Compare(fmt.Sprintf("%s", sorted[i]), fmt.Sprintf("%s", sorted[j])) < 0
	})
	return sorted
}

func isNodeEntityMatch(
	n types.Node,
	e types.Entity,
	nodeType map[string]string,
	entityType map[string][]string) bool {
	if strings.Contains(e.Table.Name, tmcf.Triple) {
		return true
	}
	nt, ok := nodeType[n.Alias]
	if !ok {
		return false
	}
	et, ok := entityType[e.Key()]
	if !ok {
		return false
	}
	for _, t := range et {
		if nt == t {
			return true
		}
	}
	return false
}

// Bind binds mapping and query statements.
func Bind(mappings []*types.Mapping, queries []*types.Query) (map[*types.Query][]*types.Mapping, error) {
	result := make(map[*types.Query][]*types.Mapping)
	queryForTriple, err := solver.MatchTriple(mappings, queries)
	if err != nil {
		return nil, err
	}
	nodeType, err := solver.GetNodeType(queries)
	if err != nil {
		return nil, err
	}
	entityType := solver.GetEntityType(mappings)

	mustMatch := map[types.Node]types.Entity{}
	for i := 0; i < 2; i++ {
		for _, q := range queries {
			result[q] = []*types.Mapping{}
			forTriple := queryForTriple[q]
			for _, m := range mappings {
				if forTriple != m.IsTriple() {
					continue
				}
				// Do not match functional deps mapping.
				if _, ok := m.Pred.(types.FuncDeps); ok {
					continue
				}
				// Do not match PlaceExt as a temporary fix to multiple mapping.
				if strings.Contains(m.Sub.Table.Name, "PlaceExt") {
					continue
				}
				// dcid query can only match to dcid mapping, not general Triples table mapping.
				if q.Pred == "dcid" && m.Pred != "dcid" {
					continue
				}
				// Strings always need to match.
				if mPred, ok := m.Pred.(string); ok && mPred != q.Pred {
					continue
				}
				// This is a typeOf mapping.
				if mObj, ok := m.Obj.(string); ok {
					if qObj, ok := q.Obj.([]string); ok && mObj != qObj[0] {
						continue
					}
					if qObj, ok := q.Obj.(string); ok && mObj != qObj {
						continue
					}
				}
				// Prune wrong type match.
				if !isNodeEntityMatch(q.Sub, m.Sub, nodeType, entityType) {
					continue
				}
				if i == 1 {
					if ent, ok := mustMatch[q.Sub]; ok {
						if m.Sub != ent && m.Sub.Table == ent.Table {
							continue
						}
					}
				}
				result[q] = append(result[q], m)
			}
			if len(result[q]) == 1 && i == 0 {
				mustMatch[q.Sub] = result[q][0].Sub
			}
		}
	}

	// If there an exact match of the must match for subject, then just keep that and discard the rest.
	for q, ms := range result {
		if ent, ok := mustMatch[q.Sub]; ok {
			for _, m := range ms {
				if m.Sub == ent {
					result[q] = []*types.Mapping{m}
				}
			}
		}
	}

	return result, nil
}

// This obtains the Cartesian product among key, value groups.
func getBindingSets(bindingMap map[*types.Query][]*types.Mapping) [][]Binding {
	result := [][]Binding{{}}
	for q, ms := range bindingMap {
		tmp := [][]Binding{}
		for _, m := range ms {
			b := Binding{q, m}
			for _, sets := range result {
				tmp = append(tmp, append(sets, b))
			}
		}
		result = tmp
	}
	return result
}

// String gets the string reprentation of a graph.
func (graph Graph) String() string {
	str := "\n"
	for key, values := range graph {
		str += fmt.Sprintf("%s\n", key)
		for v := range values {
			str += fmt.Sprintf("  %s\n", v)
		}
	}
	return str + "\n"
}

func addToGraph(g Graph, q interface{}, m interface{}, id int) {
	switch v := m.(type) {
	case types.Column:
		v.Table.ID = strconv.Itoa(id)
		m = v
	case types.Entity:
		v.Table.ID = strconv.Itoa(id)
		m = v
	}

	// Can not use slice as key, use the pointer instead.
	if strSlice, ok := q.([]string); ok {
		q = &strSlice
	}

	if _, ok := g[q]; !ok {
		g[q] = map[interface{}]struct{}{}
	}
	if _, ok := g[q][m]; !ok {
		g[q][m] = struct{}{}
	}

	if _, ok := g[m]; !ok {
		g[m] = map[interface{}]struct{}{}
	}
	if _, ok := g[m][q]; !ok {
		g[m][q] = struct{}{}
	}
}

// getGraph obtains the matching graph between query and mapping token.
func getGraph(
	bindings []Binding,
	queryID map[*types.Query]int,
	nodeRefs map[types.Node]struct{}) Graph {
	// graph holds the match relations.
	graph := Graph{}
	for _, binding := range bindings {
		m := binding.Mapping
		q := binding.Query
		id := queryID[q]
		// Predicate
		if _, ok := m.Pred.(string); !ok {
			addToGraph(graph, q.Pred, m.Pred, id)
		}

		// Subject
		addToGraph(graph, q.Sub, m.Sub, id)

		// Object
		// Need to change Triples table object_value to object_id if it matches an entity.
		// TODO(boxu): Remove this if we use a single object column in Triples table.
		mObjCopy := m.Obj
		if m.IsTriple() {
			update := false
			if q.Pred == tmcf.TypeOf {
				update = true
			} else if v, ok := q.Obj.(types.Node); ok {
				for ref := range nodeRefs {
					if v == ref {
						update = true
						break
					}
				}
			}
			if update {
				v := mObjCopy.(types.Column)
				v.Name = strings.Replace(v.Name, "object_value", "object_id", 1)
				mObjCopy = v
			}
		}
		addToGraph(graph, q.Obj, mObjCopy, id)
	}
	// Remove redundent entries.
	for key, values := range graph {
		for v := range values {
			if key == v {
				delete(values, v)
			}
		}
		if len(values) == 0 {
			delete(graph, key)
		}
	}
	return graph
}

func getFuncDepsCol(
	e types.Entity,
	funcDeps map[types.Entity]map[string]interface{}) (types.Column, error) {
	id := e.Table.ID
	e.Table.ID = "" // Unset the table id to check the func deps map.
	propCol := funcDeps[e]
	if len(propCol) != 1 {
		fmt.Printf("Multiple functional deps: %s => %s\n", e, propCol)
	}

	var col types.Column
	for _, c := range propCol {
		if v, ok := c.(types.Column); ok {
			col = v
			break
		}
	}
	col.Table.ID = id
	return col, nil
}

func pruneGraph(
	graph Graph,
	e types.Entity,
	c types.Column,
	str interface{}) {
	// Extra terms that should not be used to construct the query.
	extra := make(map[interface{}]struct{})
	for key, values := range graph {
		if key == e || key == str {
			delete(graph, key)
		} else if col, ok := key.(types.Column); ok && col.Table == c.Table {
			delete(graph, key)
			// When an entity is fully resolved, its table column to string match
			// would be redudent.
			// TODO(boxu): Revisit this when functional deps takes props other than dcid.
			remove := true
			for value := range values {
				if _, ok := value.(string); !ok {
					remove = false
					break
				}
			}
			if remove {
				for value := range values {
					extra[value] = struct{}{}
				}
			}
		}
	}
	for key, values := range graph {
		if _, ok := extra[key]; ok {
			delete(graph, key)
		} else {
			for value := range values {
				if v, ok := value.(types.Entity); ok && v == e {
					delete(values, value)
					values[str] = struct{}{}
				}
				if v, ok := value.(types.Column); ok && v == c {
					delete(values, value)
					values[str] = struct{}{}
				}
			}
		}
	}
}

// GetConstraint obtains a list of constraints object that can be used to construct SQL query.
func GetConstraint(
	graph Graph,
	funcDeps map[types.Entity]map[string]interface{}) ([]Constraint, map[types.Node]string, error) {
	// Remove unnecessary JOIN
	//
	// Assuming an entity E:Place->E1 has functional deps "dcid" that maps to C:Place->Col.id,
	// if we have the following link in the graph
	// 		C:Place->Col.id   <--->  dc/m1rl3k     [1]
	// that means this entity has been fully resolved.
	// If the Place table does not have other links in the graph, then E:Place->E1 can be
	// replaced by dc/m1rl3k everywhere to remove the unnecessary JOIN.
	// Then can also remove link [1], so it is not used as an SQL condition.
	resolvedEntities, err := graph.getResolvedEntity(funcDeps)
	if err != nil {
		return nil, nil, err
	}
	graph.prune(resolvedEntities)
	return graph.constructConstraint(funcDeps)
}

func (graph Graph) getResolvedEntity(
	funcDeps map[types.Entity]map[string]interface{}) (map[types.Entity]entityInfo, error) {
	resolvedEntities := map[types.Entity]entityInfo{}
	for key := range graph {
		if e, ok := key.(types.Entity); ok {
			// This entity has only one functional deps.
			col, err := getFuncDepsCol(e, funcDeps)
			if err != nil {
				return nil, err
			}
			if vs, ok := graph[col]; ok {
				needBreak := false
				for v := range vs {
					switch v.(type) {
					case string, *[]string:
						resolvedEntities[e] = entityInfo{e, col, v}
						needBreak = true
					}
					if needBreak {
						break
					}
				}
			}
		}
	}
	return resolvedEntities, nil
}

func (graph Graph) prune(resolvedEntities map[types.Entity]entityInfo) {
	// Gather everything that "equal" the resolved entity.
	// If there is a node in it and it matches only one entity, try to
	// use a non-resolved entity as its match.
	//
	// [E:Place->E1, C:Place->id, dc/abc123, ?dcid, ?node, E:Population->E2]
	//
	// In this case, { ?dcid => C:Place->id } should be updated to
	// { ?dcid => E:Population->E2 }
	tables := map[types.Table]struct{}{}
	for key := range graph {
		if v, ok := key.(types.Column); ok {
			tables[v.Table] = struct{}{}
		}
		if v, ok := key.(types.Entity); ok {
			tables[v.Table] = struct{}{}
		}
	}
	// No need to prune if there is only one table involved.
	if len(tables) == 1 {
		return
	}

	for ent, ei := range resolvedEntities {
		tmpEqual := []interface{}{ei.e, ei.c, ei.v}
		allEqual := map[interface{}]struct{}{}
		var curr interface{}
		for len(tmpEqual) > 0 {
			curr = tmpEqual[0]
			tmpEqual = tmpEqual[1:] // Pop one element from tmpEqual
			allEqual[curr] = struct{}{}
			for key, values := range graph {
				if key == curr {
					for v := range values {
						if _, ok := allEqual[v]; !ok {
							tmpEqual = append(tmpEqual, v)
						}
					}
				}
			}
		}
		// Find alternative entity to use.
		var alt interface{}
		items := sortMapSet(allEqual) // Sort to get stable results.
		for _, item := range items {
			if e, ok := item.(types.Entity); ok {
				if _, ok := resolvedEntities[e]; !ok {
					alt = e
					break
				}
			}
			// Check InstanceQueryFipsIdContainedIn test case
			if c, ok := item.(types.Column); ok {
				if c.Table != ent.Table {
					alt = c
					break
				}
			}
		}
		if alt == nil {
			continue
		}
		for key, values := range graph {
			if _, ok := key.(types.Node); ok && len(values) == 1 {
				if _, ok := allEqual[key]; ok {
					graph[key] = map[interface{}]struct{}{alt: {}}
				}
			}
		}
		ei := resolvedEntities[ent]
		e := ei.e
		c := ei.c
		v := ei.v
		replace := true
		for key := range graph {
			if key != e && key != c {
				if ent, ok := key.(types.Entity); ok && ent.Table == e.Table {
					replace = false
					break
				}
			}
		}
		if replace {
			pruneGraph(graph, e, c, v)
		}
	}
}

func (graph Graph) constructConstraint(
	funcDeps map[types.Entity]map[string]interface{}) (
	[]Constraint, map[types.Node]string, error) {
	// Pick edges from graph and use as constraints.
	// Only need to pick the key with type Node and string.
	result := []Constraint{}
	constNode := map[types.Node]string{}
	for key, values := range graph {
		// Sort values to get consistent result.
		sorted := sortMapSet(values)
		// Key is a Node.
		if _, ok := key.(types.Node); ok {
			var c types.Column
			if len(sorted) == 1 {
				value := sorted[0]
				if v, ok := value.(types.Entity); ok {
					col, err := getFuncDepsCol(v, funcDeps)
					if err != nil {
						return nil, nil, err
					}
					c = col
				} else if col, ok := value.(types.Column); ok {
					c = col
				} else {
					continue
				}
				result = append(result, Constraint{c, key})
			} else {
				// Loop through values and get one item to use as the pivot value.
				for _, value := range sorted {
					if v, ok := value.(types.Column); ok {
						c = v
						break
					}
					if v, ok := value.(types.Entity); ok {
						col, err := getFuncDepsCol(v, funcDeps)
						if err != nil {
							return nil, nil, err
						}
						c = col
						break
					}
				}
				result = append(result, Constraint{c, key})
				// Loop through values the second time to form the constraints.
				for _, value := range sorted {
					if value != c {
						if v, ok := value.(types.Entity); ok {
							col, err := getFuncDepsCol(v, funcDeps)
							if err != nil {
								return nil, nil, err
							}
							value = col
						}
						if value != c {
							if strArr, ok := value.(*[]string); ok {
								result = append(result, Constraint{c, *strArr})
							} else {
								result = append(result, Constraint{c, value})
							}
						}
					}
				}
			}
		}
		// If key is a string, each match forms a constraint.
		if v, ok := key.(string); ok {
			for _, value := range sorted {
				if col, ok := value.(types.Column); ok {
					result = append(result, Constraint{col, key})
				} else if n, ok := value.(types.Node); ok {
					constNode[n] = v
				} else if ent, ok := value.(types.Entity); ok {
					col, err := getFuncDepsCol(ent, funcDeps)
					if err != nil {
						return nil, nil, err
					}
					result = append(result, Constraint{col, key})
				} else {
					return nil, nil, status.Errorf(
						codes.InvalidArgument, "String should match a column or node, get %v", value)
				}
			}
		}
		// If key is a slice of string, each match form a constraint.
		if strSlice, ok := key.(*[]string); ok {
			for _, value := range sorted {
				if col, ok := value.(types.Column); ok {
					result = append(result, Constraint{col, *strSlice})
				} else {
					return nil, nil, status.Errorf(
						codes.InvalidArgument, "String should match a column, get %v", value)
				}
			}
		}
	}
	return result, constNode, nil
}

func removeConstraints(
	jc map[types.Table][]Constraint,
	t types.Table,
	c Constraint,
) map[types.Table][]Constraint {
	cs := jc[t]
	for i, _c := range cs {
		if _c == c {
			cs[i] = cs[len(cs)-1]
			cs = cs[:len(cs)-1]
			break
		}
	}
	jc[t] = cs
	return jc
}

func getBqQuery(opts *types.QueryOptions) (bigquery.Client.Query, []bigquery.QueryParameter){
	var queryParams []bigquery.QueryParameter
	var query bigquery.Client.Query

	queryStr += "SELECT DISTINCT "


	
	if opts.Orderby != "" {
		orderby := strings.TrimPrefix(strings.ReplaceAll(opts.Orderby, "/", "_"), "?")
		if opts.ASC {
			orderby += " ASC\n"
		} else {
			orderby += " DESC\n"
		}
		sql += fmt.Sprintf("ORDER BY @order_by")
		queryParams = append(queryParams, bigquery.QueryParameter{Name: "order_by", Value: orderby})
	}

	if opts.Limit > 0 {
		queryParams = append(queryParams, bigquery.QueryParameter{Name: "limit", Value: opts.Limit})
		sql += fmt.Sprintf("LIMIT @limit\n", opts.Limit)
	}

	q := *bigquery.Client.Query(queryStr)
	q.parameters = queryParams

	return queryParams
}

func getSQL(
	nodes []types.Node,
	constraints []Constraint,
	constNode map[types.Node]string,
	provInfo ProvInfo,
	opts *types.QueryOptions) (string, map[int][]int, error) {
	// prov maps provenance column to node columns
	prov := map[int][]int{}
	provCols := map[types.Column]int{}
	provList := []types.Column{}
	pc := len(nodes)
	sql := "SELECT "
	if opts.Distinct {
		sql += "DISTINCT "
	}
	for idx, n := range nodes {
		if idx != 0 {
			sql += ",\n"
		}
		if str, ok := constNode[n]; ok {
			sql += fmt.Sprintf(`"%s"`, str)
		}
		for _, c := range constraints {
			if n == c.RHS {
				sql += fmt.Sprintf("%s.%s AS %s",
					c.LHS.Table.Alias(),
					c.LHS.Name,
					strings.TrimPrefix(strings.ReplaceAll(n.Alias, "/", "_"), "?"))
				if provInfo.query {
					if provCol, ok := provInfo.tableProv[c.LHS.Table.Name]; ok {
						provCol.Table.ID = c.LHS.Table.ID
						if i, ok := provCols[provCol]; ok {
							prov[i] = append(prov[i], idx)
						} else {
							provList = append(provList, provCol)
							provCols[provCol] = pc
							prov[pc] = []int{idx}
							pc++
						}
					}
				}
				break
			}
		}
	}
	for i, p := range provList {
		sql += ",\n" + fmt.Sprintf("%s.%s AS prov%d", p.Table.Alias(), p.Name, i)
	}

	tableCounter := map[types.Table]int{}
	constCounter := map[types.Table]int{}
	joinConstraints := map[types.Table][]Constraint{}
	whereConstraints := []Constraint{}

	for _, c := range constraints {
		tableCounter[c.LHS.Table]++
		switch v := c.RHS.(type) {
		case types.Column:
			joinConstraints[c.LHS.Table] = append(joinConstraints[c.LHS.Table], c)
			joinConstraints[v.Table] = append(joinConstraints[v.Table], c)
			tableCounter[v.Table]++
		case types.Node:
		default:
			whereConstraints = append(whereConstraints, c)
			constCounter[c.LHS.Table]++
		}
	}

	// Sort the join conditions for each table by the joined table's importance,
	// ie, the table counter count.
	for t, cs := range joinConstraints {
		sort.SliceStable(cs, func(i, j int) bool {
			var t1, t2 types.Table
			if cs[i].LHS.Table == t {
				t1 = cs[i].RHS.(types.Column).Table
			} else {
				t1 = cs[i].LHS.Table
			}
			if cs[j].LHS.Table == t {
				t2 = cs[j].RHS.(types.Column).Table
			} else {
				t2 = cs[j].LHS.Table
			}
			if tableCounter[t1] == tableCounter[t2] {
				return strings.Compare(t1.String(), t2.String()) < 0
			}
			return tableCounter[t1] > tableCounter[t2]
		})
		joinConstraints[t] = cs
	}

	// In the following section, build the "JOIN" relations from "currTable".
	// This is conceptually building a "join graph" and keep extending it based on
	// the `joinConstraints`.

	// Choose the table with the most constant constraints as the starting table.
	var currTable types.Table
	maxCount := 0
	for t, count := range constCounter {
		if count > maxCount || (count == maxCount && t.String() < currTable.String()) {
			maxCount = count
			currTable = t
		}
	}
	// When there is no constant an no join, need to pick the currTable.
	if (types.Table{}) == currTable {
		for _, c := range constraints {
			currTable = c.LHS.Table
			break
		}
	}

	sql += fmt.Sprintf("\nFROM %s AS %s\n", currTable.Name, currTable.Alias())

	// Keep track of table that has been processed, they should already have an
	// alias in SQL and could be used as "currTable".
	processedTable := map[types.Table]struct{}{}

	var currCol, otherCol types.Column
	for len(joinConstraints) > 0 {
		futureTables := []types.Table{}
		processedTable[currTable] = struct{}{}
		for _, c := range joinConstraints[currTable] {
			if currTable == c.LHS.Table {
				currCol = c.LHS
				otherCol = c.RHS.(types.Column)
			} else {
				currCol = c.RHS.(types.Column)
				otherCol = c.LHS
			}
			if _, ok := processedTable[otherCol.Table]; ok {
				whereConstraints = append(whereConstraints, c)
			} else {
				sql += fmt.Sprintf("JOIN %s AS %s\n", otherCol.Table.Name, otherCol.Table.Alias())
				sql += fmt.Sprintf(
					"ON %s.%s = %s.%s\n",
					currCol.Table.Alias(), currCol.Name, otherCol.Table.Alias(), otherCol.Name)
				processedTable[otherCol.Table] = struct{}{}

			}
			// Remove the processed constraints
			joinConstraints = removeConstraints(joinConstraints, currTable, c)
			joinConstraints = removeConstraints(joinConstraints, otherCol.Table, c)
			if len(joinConstraints[currCol.Table]) == 0 {
				delete(joinConstraints, currCol.Table)
			}
			if len(joinConstraints[otherCol.Table]) == 0 {
				delete(joinConstraints, otherCol.Table)
			}
			// Add the other table to `futureTables`, which all have been processed
			// at least once and can be used as `currTable` to further extend the
			// "join graph"
			futureTables = append(futureTables, otherCol.Table)
		}

		if len(futureTables) > 0 {
			// Pick a new `currTable` from `futureTables` (if non-empty)
			for _, v := range futureTables {
				if _, ok := joinConstraints[v]; ok {
					currTable = v
					break
				}
			}
		} else {
			// It's possible `futureTables` have all matched and become empty.
			// In this case, pick currTable from joinConstraints. `currTable` needs
			// to be a table that has already been processed, to extend the join graph.
			for t := range joinConstraints {
				if _, ok := processedTable[t]; ok {
					currTable = t
					break
				}
			}
		}
	}

	// Sort to get deterministic result.
	sort.SliceStable(whereConstraints, func(i, j int) bool {
		l := whereConstraints[i].LHS
		r := whereConstraints[j].LHS
		// Put "variable_measured" constraints at the beginning to better use
		// StatVarObservation's key.
		if l.Name == svProp {
			return true
		}
		if r.Name == svProp {
			return false
		}
		return strings.Compare(l.String(), r.String()) < 0
	})
	for idx, c := range whereConstraints {
		if idx == 0 {
			sql += "WHERE "
		} else if idx != len(whereConstraints) {
			sql += "AND "
		}
		switch v := c.RHS.(type) {
		case types.Column:
			sql += fmt.Sprintf("%s.%s = %s.%s\n", c.LHS.Table.Alias(), c.LHS.Name, v.Table.Alias(), v.Name)
		case string:
			// Before we have spanner table reflection, need to hardcode check here.
			// But the user should really have quote for strings.
			useQuote := strings.Contains(c.LHS.Table.Name, tmcf.Triple)
			sql += fmt.Sprintf("%s.%s = %s\n", c.LHS.Table.Alias(), c.LHS.Name, addQuote(v, useQuote))
		case []string:
			strs := []string{}
			for _, s := range v {
				strs = append(strs, addQuote(s))
			}
			sql += fmt.Sprintf("%s.%s IN (%s)\n", c.LHS.Table.Alias(), c.LHS.Name, strings.Join(strs, ", "))
		}
	}
	return sql, prov, nil
}

// Translate takes a datalog query and translates to GoogleSQL query based on schema mapping.
func Translate(
	mappings []*types.Mapping, nodes []types.Node, queries []*types.Query,
	subTypeMap map[string]string, options ...*types.QueryOptions) (
	*Translation, error) {
	funcDeps, err := solver.GetFuncDeps(mappings)
	if err != nil {
		return nil, err
	}

	tableProv, err := solver.GetProvColumn(mappings)
	if err != nil {
		return nil, err
	}

	mappings = solver.PruneMapping(mappings)
	queries = solver.RewriteQuery(queries, subTypeMap)
	matchTriple, err := solver.MatchTriple(mappings, queries)
	if err != nil {
		return nil, err
	}
	queryID := solver.GetQueryID(queries, matchTriple)

	bindingMap, err := Bind(mappings, queries)
	if err != nil {
		return nil, err
	}
	bindingSets := getBindingSets(bindingMap)
	if len(bindingSets) > 1 {
		fmt.Printf("There are %d binding sets\n", len(bindingSets))
	} else if len(bindingSets) == 0 {
		return nil, status.Errorf(codes.Internal, "Failed to get translation result")
	}

	nodeRefs := solver.GetNodeRef(queries)
	graph := getGraph(bindingSets[0], queryID, nodeRefs)
	constraints, constNode, err := GetConstraint(graph, funcDeps)
	if err != nil {
		return nil, err
	}

	var (
		queryProv    bool
		queryOptions *types.QueryOptions
	)
	if len(options) > 0 {
		queryOptions = options[0]
		queryProv = options[0].Prov
	} else {
		queryOptions = &types.QueryOptions{}
	}

	sql, prov, err := getSQL(
		nodes,
		constraints,
		constNode,
		ProvInfo{queryProv, tableProv},
		queryOptions,
	)
	if err != nil {
		return nil, err
	}
	return &Translation{sql, nodes, bindingSets[0], constraints, prov}, nil
}
