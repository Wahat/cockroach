// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/sql/roleprivilege"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// alterUserSetPasswordNode represents an ALTER USER ... WITH PASSWORD statement.
type alterRoleNode struct {
	name           func() (string, error)
	ifExists       bool
	rolePrivileges roleprivilege.List

	run alterRoleRun
}

// AlterUserSetPassword changes a user's password.
// Privileges: UPDATE on the users table.
func (p *planner) AlterRolePrivileges(
	ctx context.Context, n *tree.AlterRolePrivileges,
) (*alterRoleNode, error) {
	tDesc, err := ResolveExistingObject(ctx, p, userTableName, tree.ObjectLookupFlagsWithRequired(), ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}

	// Note that for Postgres, only superuser can ALTER another superuser
	// CockroachDB does not support superuser privilege right now
	// However we make it so the admin role cannot be edited (done in startExec)
	if err := p.HasCreateRolePrivilege(ctx); err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, tDesc, privilege.UPDATE); err != nil {
		return nil, err
	}

	name, err := p.TypeAsString(n.Name, "ALTER ROLE")
	if err != nil {
		return nil, err
	}

	return &alterRoleNode{
		name:           name,
		ifExists:       n.IfExists,
		rolePrivileges: n.RolePrivileges,
	}, nil
}

// alterRoleRun is the run-time state of
// alterRoleRun	 for local execution.
type alterRoleRun struct {
	rowsAffected int
}

func (n *alterRoleNode) startExec(params runParams) error {
	name, err := n.name()
	if err != nil {
		return err
	}
	if name == "" {
		return errNoUserNameSpecified
	}
	if name == "admin" {
		return errors.New("Cannot edit admin role")
	}
	normalizedUsername, err := NormalizeAndValidateUsername(name)
	if err != nil {
		return err
	}

	n.run.rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		"update-role",
		params.p.txn,
		// parameterize permissions using map and function WIP (for when extra role privileges are added)
		`UPDATE system.users SET "hasCreateRole" = $2 WHERE username = $1`,
		normalizedUsername,
		n.rolePrivileges.ToBitField()&roleprivilege.CREATEROLE.Mask() != 0,
	)
	if err != nil {
		return err
	}
	if n.run.rowsAffected == 0 && !n.ifExists {
		return pgerror.Newf(pgcode.UndefinedObject,
			"user %s does not exist", n.name)
	}
	return err
}

func (*alterRoleNode) Next(runParams) (bool, error) { return false, nil }
func (*alterRoleNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterRoleNode) Close(context.Context)        {}

func (n *alterRoleNode) FastPathResults() (int, bool) {
	return n.run.rowsAffected, true
}
