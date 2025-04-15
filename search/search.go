// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"github.com/parquet-go/parquet-go"
)

type Constraint interface {
	// rowRanges returns a set of non-overlapping increasing row indexes that may satisfy the constraint.
	rowRanges(rg parquet.RowGroup, rr []rowRange) ([]rowRange, error)
	// init initializes the constraint with respect to the file schema and projections.
	init(s *parquet.Schema) error
}
