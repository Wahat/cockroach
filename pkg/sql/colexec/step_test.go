// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestStep(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		stepSize uint64
		tuples   []tuple
		expected []tuple
	}{
		{
			stepSize: 1,
			tuples:   tuples{{1}},
			expected: tuples{{1}},
		},
		{
			stepSize: 100000,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}},
		},
		{
			stepSize: 2,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}, {3}},
		},
		{
			stepSize: 3,
			tuples:   tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}},
			expected: tuples{{1}, {4}, {7}, {10}},
		},
	}

	for _, tc := range tcs {
		// The tuples consisting of all nulls still count as separate rows, so if
		// we replace all values with nulls, we should get the same output.
		runTestsWithoutAllNullsInjection(t, []tuples{tc.tuples}, nil /* typs */, tc.expected, orderedVerifier, func(input []Operator) (Operator, error) {
			return NewStepOp(input[0], tc.stepSize), nil
		})
	}
}

func BenchmarkStepOp(b *testing.B) {
	ctx := context.Background()
	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64})
	batch.SetLength(coldata.BatchSize())
	source := NewRepeatableBatchSource(batch)
	source.Init()

	o := NewStepOp(source, 5)
	// Set throughput proportional to size of the selection vector.
	b.SetBytes(int64(2 * coldata.BatchSize()))
	for i := 0; i < b.N; i++ {
		o.(*stepOp).Reset()
		o.Next(ctx)
	}
}
