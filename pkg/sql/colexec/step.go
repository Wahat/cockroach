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
)

// stepOp is an operator that implements step, returning
// every n rows
type stepOp struct {
	OneInputNode

	stepSize uint64

	// Track which rows to take
	curr uint64
}

var _ Operator = &stepOp{}

// NewLimitOp returns a new limit operator with the given limit.
func NewStepOp(input Operator, stepSize uint64) Operator {
	c := &stepOp{
		OneInputNode: NewOneInputNode(input),
		stepSize:     stepSize,
		curr:         0,
	}
	return c
}

func (c *stepOp) Init() {
	c.input.Init()
}

func (c *stepOp) Next(ctx context.Context) coldata.Batch {
	var bat coldata.Batch
	var newLength int16
	for {
		bat = c.input.Next(ctx)
		length := bat.Length()

		if length == 0 {
			return bat
		}

		selectionVector := bat.Selection()
		newLength = 0

		if selectionVector != nil {
			newSelectionVector := make([]uint16, length)

			for i := uint16(0); i < length; i++ {
				if c.curr%c.stepSize == 0 {
					newSelectionVector[newLength] = selectionVector[i]
					newLength++
				}
				c.curr++
			}

			copy(selectionVector, newSelectionVector)
		} else {
			bat.SetSelection(true)
			selectionVector = bat.Selection()

			for i := range selectionVector[:length] {
				//for i := uint16(0); i < length; i++ {
				if c.curr%c.stepSize == 0 {
					selectionVector[newLength] = uint16(i)
					newLength++
				}
				c.curr++
			}
		}

		bat.SetLength(uint16(newLength))
		if newLength != 0 {
			break
		}
	}
	return bat
}

// Reset resets the stepOp for another run. Primarily used for
// benchmarks.
func (c *stepOp) Reset() {
	c.curr = 0
}
