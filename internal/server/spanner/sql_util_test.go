package spanner

import (
	"strings"
	"testing"

	cloudSpanner "cloud.google.com/go/spanner"
)

func TestUnrollParameters(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		params  map[string]interface{}
		wantSQL string
		wantNew map[string]interface{} // new params that should be added (subset check)
	}{
		// --- IN UNNEST cases ---
		{
			name:    "IN UNNEST single element -> equality",
			sql:     "WHERE x IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a"}},
			wantSQL: "WHERE x = @vals_0",
			wantNew: map[string]interface{}{"vals_0": "a"},
		},
		{
			name:    "IN UNNEST multiple elements -> IN list",
			sql:     "WHERE x IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "WHERE x IN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		{
			name:    "IN UNNEST at start of string",
			sql:     "IN UNNEST(@vals) AS x",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "IN (@vals_0,@vals_1) AS x",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		{
			name:    "JOIN UNNEST should NOT be unrolled",
			sql:     "CROSS JOIN UNNEST(@vals) AS ent",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "CROSS JOIN UNNEST(@vals) AS ent",
			wantNew: map[string]interface{}{},
		},
		{
			name:    "JOIN UNNEST single element should NOT be unrolled",
			sql:     "CROSS JOIN UNNEST(@vals) AS ent",
			params:  map[string]interface{}{"vals": []string{"a"}},
			wantSQL: "CROSS JOIN UNNEST(@vals) AS ent",
			wantNew: map[string]interface{}{},
		},
		{
			name:    "FROM UNNEST should NOT be unrolled",
			sql:     "FROM UNNEST(@vals) AS var",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "FROM UNNEST(@vals) AS var",
			wantNew: map[string]interface{}{},
		},
		{
			name:    "ON ... IN UNNEST (ON prefix should not match)",
			sql:     "ON t.var IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "ON t.var IN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		{
			name:    "AND ... IN UNNEST",
			sql:     "AND x IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "AND x IN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		{
			name:    "WHERE ... IN UNNEST",
			sql:     "WHERE x IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "WHERE x IN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		{
			name:    "Multiple IN UNNEST with same param",
			sql:     "WHERE x IN UNNEST(@vals) AND y IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "WHERE x IN (@vals_0,@vals_1) AND y IN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		{
			name:    "Multiple IN UNNEST with different params",
			sql:     "WHERE x IN UNNEST(@vars) AND y IN UNNEST(@ents)",
			params:  map[string]interface{}{"vars": []string{"a"}, "ents": []string{"b", "c"}},
			wantSQL: "WHERE x = @vars_0 AND y IN (@ents_0,@ents_1)",
			wantNew: map[string]interface{}{"vars_0": "a", "ents_0": "b", "ents_1": "c"},
		},
		{
			name:    "Mixed: IN UNNEST and JOIN UNNEST with same param",
			sql:     "CROSS JOIN UNNEST(@vals) AS ent WHERE x IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "CROSS JOIN UNNEST(@vals) AS ent WHERE x IN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		// --- NOT IN UNNEST cases ---
		{
			name:    "NOT IN UNNEST single element -> !=",
			sql:     "WHERE x NOT IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a"}},
			wantSQL: "WHERE x != @vals_0",
			wantNew: map[string]interface{}{"vals_0": "a"},
		},
		{
			name:    "NOT IN UNNEST multiple elements -> NOT IN list",
			sql:     "WHERE x NOT IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "WHERE x NOT IN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		{
			name:    "NOT IN UNNEST at start of string",
			sql:     "NOT IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "NOT IN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		{
			name:    "AND NOT IN UNNEST single element",
			sql:     "AND x NOT IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a"}},
			wantSQL: "AND x != @vals_0",
			wantNew: map[string]interface{}{"vals_0": "a"},
		},
		// --- Case insensitivity ---
		{
			name:    "lowercase in unnest",
			sql:     "WHERE x in unnest(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "WHERE x IN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		{
			name:    "mixed case NOT IN UNNEST",
			sql:     "WHERE x Not In UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a"}},
			wantSQL: "WHERE x != @vals_0",
			wantNew: map[string]interface{}{"vals_0": "a"},
		},
		// --- Edge cases ---
		{
			name:    "Empty array should not be unrolled",
			sql:     "WHERE x IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{}},
			wantSQL: "WHERE x IN UNNEST(@vals)",
			wantNew: map[string]interface{}{},
		},
		{
			name:    "Array > 10 should not be unrolled",
			sql:     "WHERE x IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}},
			wantSQL: "WHERE x IN UNNEST(@vals)",
			wantNew: map[string]interface{}{},
		},
		{
			name:    "Exactly 10 elements should be unrolled",
			sql:     "WHERE x IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}},
			wantSQL: "WHERE x IN (@vals_0,@vals_1,@vals_2,@vals_3,@vals_4,@vals_5,@vals_6,@vals_7,@vals_8,@vals_9)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_9": "j"},
		},
		{
			name:    "Non-string array param should not be unrolled",
			sql:     "WHERE x IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []float64{1.0, 2.0}},
			wantSQL: "WHERE x IN UNNEST(@vals)",
			wantNew: map[string]interface{}{},
		},
		{
			name:    "No UNNEST in SQL should be a no-op",
			sql:     "SELECT * FROM Foo WHERE x = @val",
			params:  map[string]interface{}{"val": "test", "vals": []string{"a", "b"}},
			wantSQL: "SELECT * FROM Foo WHERE x = @val",
			wantNew: map[string]interface{}{},
		},
		{
			name:    "Param key with special regex chars should be safe",
			sql:     "WHERE x IN UNNEST(@val.1)",
			params:  map[string]interface{}{"val.1": []string{"a", "b"}},
			wantSQL: "WHERE x IN (@val.1_0,@val.1_1)",
			wantNew: map[string]interface{}{"val.1_0": "a", "val.1_1": "b"},
		},
		{
			name:    "Newline-separated IN UNNEST",
			sql:     "WHERE\nx\nIN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "WHERE\nx\nIN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		{
			name:    "Tab-separated IN UNNEST",
			sql:     "WHERE\tx\tIN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "WHERE\tx\tIN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals_0": "a", "vals_1": "b"},
		},
		{
			name:    "Original param still present after unrolling",
			sql:     "WHERE x IN UNNEST(@vals)",
			params:  map[string]interface{}{"vals": []string{"a", "b"}},
			wantSQL: "WHERE x IN (@vals_0,@vals_1)",
			wantNew: map[string]interface{}{"vals": []string{"a", "b"}, "vals_0": "a", "vals_1": "b"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := &cloudSpanner.Statement{
				SQL:    tc.sql,
				Params: tc.params,
			}
			UnrollParameters(stmt)

			if stmt.SQL != tc.wantSQL {
				t.Errorf("SQL mismatch:\n  got:  %q\n  want: %q", stmt.SQL, tc.wantSQL)
			}

			// Verify expected new params exist
			for k, v := range tc.wantNew {
				got, ok := stmt.Params[k]
				if !ok {
					t.Errorf("missing param %q in result params: %v", k, stmt.Params)
					continue
				}
				// For []string comparison, use reflect.DeepEqual via fmt
				gotStr := toString(got)
				wantStr := toString(v)
				if gotStr != wantStr {
					t.Errorf("param %q mismatch:\n  got:  %v\n  want: %v", k, got, v)
				}
			}
		})
	}
}

func TestInterpolateSQL(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		params map[string]interface{}
		want   string
	}{
		{
			name:   "JOIN UNNEST preserved with IN UNNEST unrolled",
			sql:    "FROM UNNEST(@vars) AS var CROSS JOIN UNNEST(@ents) AS ent WHERE x IN UNNEST(@ents)",
			params: map[string]interface{}{"vars": []string{"v1"}, "ents": []string{"e1", "e2"}},
			want:   "FROM UNNEST(['v1']) AS var CROSS JOIN UNNEST(['e1','e2']) AS ent WHERE x IN ('e1','e2')",
		},
		{
			name:   "NOT IN UNNEST single element",
			sql:    "WHERE x NOT IN UNNEST(@vals)",
			params: map[string]interface{}{"vals": []string{"a"}},
			want:   "WHERE x != 'a'",
		},
		{
			name:   "NOT IN UNNEST multiple elements",
			sql:    "WHERE x NOT IN UNNEST(@vals)",
			params: map[string]interface{}{"vals": []string{"a", "b"}},
			want:   "WHERE x NOT IN ('a','b')",
		},
		{
			name:   "JOIN UNNEST with single element (should keep UNNEST)",
			sql:    "CROSS JOIN UNNEST(@ents) AS ent",
			params: map[string]interface{}{"ents": []string{"e1"}},
			want:   "CROSS JOIN UNNEST(['e1']) AS ent",
		},
		{
			name:   "String with single quote escaped",
			sql:    "WHERE x IN UNNEST(@vals)",
			params: map[string]interface{}{"vals": []string{"it's"}},
			want:   "WHERE x = 'it''s'",
		},
		{
			name:   "Mixed JOIN UNNEST + NOT IN UNNEST + IN UNNEST",
			sql:    "CROSS JOIN UNNEST(@ents) AS ent WHERE x IN UNNEST(@vars) AND y NOT IN UNNEST(@vars)",
			params: map[string]interface{}{"ents": []string{"e1", "e2"}, "vars": []string{"v1", "v2"}},
			want:   "CROSS JOIN UNNEST(['e1','e2']) AS ent WHERE x IN ('v1','v2') AND y NOT IN ('v1','v2')",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := &cloudSpanner.Statement{
				SQL:    tc.sql,
				Params: tc.params,
			}
			got := InterpolateSQL(stmt)
			if got != tc.want {
				t.Errorf("SQL mismatch:\n  got:  %q\n  want: %q", got, tc.want)
			}
		})
	}
}

// toString is a helper for comparing param values that may be string or []string.
func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []string:
		return strings.Join(val, ",")
	default:
		return ""
	}
}
