// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestStepProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	testCases := []struct {
		stepSize int
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			stepSize: 1,
			input: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[3]},
				{v[2], v[6]},
				{v[2], v[9]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[6], v[6]},
				{v[7], v[6]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[3]},
				{v[2], v[6]},
				{v[2], v[9]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[6], v[6]},
				{v[7], v[6]},
			},
		},
		{
			stepSize: 2,
			input: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[5], v[6]},
				{v[2], v[3]},
				{v[5], v[6]},
				{v[2], v[6]},
				{v[3], v[5]},
				{v[2], v[9]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[3]},
				{v[2], v[6]},
				{v[2], v[9]},
			},
		},
		{
			stepSize: 3,
			input: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[3]},
				{v[2], v[6]},
				{v[2], v[9]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[5], v[6]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[5], v[6]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[5], v[6]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[9]},
				{v[5], v[6]},
				{v[5], v[6]},
				{v[5], v[6]},
			},
		},
		{
			stepSize: 20,
			input: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[2], v[3]},
				{v[2], v[6]},
				{v[2], v[9]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[5], v[6]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[5], v[6]},
				{v[3], v[5]},
				{v[5], v[6]},
				{v[5], v[6]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
			},
		},
		{
			stepSize: 20,
			input:    sqlbase.EncDatumRows{},
			expected: sqlbase.EncDatumRows{},
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {

			out := &distsqlutils.RowBuffer{}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())

			flowCtx := &execinfra.FlowCtx{
				Cfg:     &execinfra.ServerConfig{Settings: st},
				EvalCtx: &evalCtx,
			}

			//input := execinfra.NewRepeatableRowSource(cols, c.input)
			input := distsqlutils.NewRowBuffer(sqlbase.TwoIntCols, c.input, distsqlutils.RowBufferArgs{})

			s, err := newStepProcessor(flowCtx, 0 /* processorID */, input, &execinfrapb.PostProcessSpec{}, out, uint32(c.stepSize))
			if err != nil {
				t.Fatal(err)
			}

			s.Run(context.Background())
			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}
			var res sqlbase.EncDatumRows
			for {
				row := out.NextNoMeta(t).Copy()
				if row == nil {
					break
				}
				res = append(res, row)
			}

			if result := res.String(sqlbase.TwoIntCols); result != c.expected.String(sqlbase.TwoIntCols) {
				t.Errorf("invalid results: %s, expected %s'", result, c.expected.String(sqlbase.TwoIntCols))
			}
		})
	}
}

func BenchmarkStep(b *testing.B) {
	const numRows = 1 << 16

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	post := &execinfrapb.PostProcessSpec{}
	disposer := &execinfra.RowDisposer{}
	for _, numCols := range []int{1, 1 << 1, 1 << 2, 1 << 4, 1 << 8} {
		b.Run(fmt.Sprintf("cols=%d", numCols), func(b *testing.B) {
			cols := make([]types.T, numCols)
			for i := range cols {
				cols[i] = *types.Int
			}
			input := execinfra.NewRepeatableRowSource(cols, sqlbase.MakeIntRows(numRows, numCols))

			b.SetBytes(int64(8 * numRows * numCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d, err := newStepProcessor(flowCtx, 0 /* processorID */, input, post, disposer, 5)
				if err != nil {
					b.Fatal(err)
				}
				d.Run(context.Background())
				input.Reset()
			}
		})
	}
}
