// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"bytes"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func buildFile[T any](t testing.TB, rows []T) *parquet.File {
	buf := bytes.NewBuffer(nil)
	w := parquet.NewGenericWriter[T](buf, parquet.PageBufferSize(12), parquet.WriteBufferSize(0))
	for _, row := range rows {
		if _, err := w.Write([]T{row}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(buf.Bytes())
	file, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func TestEqual(t *testing.T) {
	type expectation struct {
		constraints []Constraint
		expect      []rowRange
	}
	type testcase[T any] struct {
		rows         []T
		expectations []expectation
	}

	t.Run("", func(t *testing.T) {
		type s struct {
			A int64  `parquet:",optional,dict"`
			B int64  `parquet:",optional,dict"`
			C string `parquet:",optional,dict"`
		}
		for _, tt := range []testcase[s]{
			{
				rows: []s{
					{
						A: 1,
						B: 2,
						C: "a",
					},
					{
						A: 3,
						B: 4,
						C: "b",
					},
					{
						A: 7,
						B: 12,
						C: "c",
					},
					{
						A: 9,
						B: 22,
						C: "d",
					},
					{
						A: 0,
						B: 1,
						C: "e",
					},
					{
						A: 7,
						B: 1,
						C: "f",
					},
					{
						A: 7,
						B: 1,
						C: "g",
					},
					{
						A: 0,
						B: 1,
						C: "h",
					},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf(7)),
							Equal("C", parquet.ValueOf("g")),
						},
						expect: []rowRange{
							{from: 6, count: 1},
						},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf(7)),
						},
						expect: []rowRange{
							{from: 2, count: 1},
							{from: 5, count: 2},
						},
					},
				},
			},
		} {

			sfile := buildFile(t, tt.rows)
			for _, expectation := range tt.expectations {
				t.Run("", func(t *testing.T) {
					if err := initialize(sfile.Schema(), expectation.constraints...); err != nil {
						t.Fatal(err)
					}
					for _, rg := range sfile.RowGroups() {
						rr, err := filter(rg, expectation.constraints...)
						if err != nil {
							t.Fatal(err)
						}
						if !slices.Equal(rr, expectation.expect) {
							t.Fatalf("expected %+v, got %+v", expectation.expect, rr)
						}
					}
				})
			}
		}
	})
}
