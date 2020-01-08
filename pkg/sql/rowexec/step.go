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

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// noopProcessor is a processor that simply passes rows through from the
// synchronizer to the post-processing stage. It can be useful for its
// post-processing or in the last stage of a computation, where we may only
// need the synchronizer to join streams.
type stepProcessor struct {
	execinfra.ProcessorBase
	input execinfra.RowSource
	stepSize uint32
	count uint32
}

var _ execinfra.Processor = &stepProcessor{}
var _ execinfra.RowSource = &stepProcessor{}

const stepProcName = "step"

func newStepProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	stepSize uint32,
) (*stepProcessor, error) {
	s := &stepProcessor{input: input, stepSize: stepSize, count: 0}
	if err := s.Init(
		s,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{s.input}},
	); err != nil {
		return nil, err
	}
	return s, nil
}

// Start is part of the RowSource interface.
func (s *stepProcessor) Start(ctx context.Context) context.Context {
	s.input.Start(ctx)
	return s.StartInternal(ctx, noopProcName)
}

// Next is part of the RowSource interface.
func (s *stepProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for s.State == execinfra.StateRunning {
		row, meta := s.input.Next()

		if meta != nil {
			if meta.Err != nil {
				s.MoveToDraining(nil /* err */)
			}

			if s.count % s.stepSize == 0 {
				s.count++
				return nil, meta
			}
		}
		if row == nil {
			s.MoveToDraining(nil /* err */)
			break
		}

		if s.count % s.stepSize == 0 {
			s.count++
			if outRow := s.ProcessRowHelper(row); outRow != nil {
				return outRow, nil
			}
		}
		s.count++
	}
	return nil, s.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (n *stepProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	n.InternalClose()
}
