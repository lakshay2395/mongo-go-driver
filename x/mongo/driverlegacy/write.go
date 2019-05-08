// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package driverlegacy

import (
	"context"

	"github.com/lakshay2395/mongo-go-driver/bson"
	"github.com/lakshay2395/mongo-go-driver/mongo/writeconcern"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/session"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/topology"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/uuid"
	"github.com/lakshay2395/mongo-go-driver/x/network/command"
	"github.com/lakshay2395/mongo-go-driver/x/network/description"
)

// Write handles the full cycle dispatch and execution of a write command against the provided
// topology.
func Write(
	ctx context.Context,
	cmd command.Write,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
) (bson.Raw, error) {

	if cmd.Session != nil && cmd.Session.PinnedServer != nil {
		selector = cmd.Session.PinnedServer
	}
	ss, err := topo.SelectServerLegacy(ctx, selector)
	if err != nil {
		return nil, err
	}

	desc := ss.Description()
	conn, err := ss.ConnectionLegacy(ctx)
	if err != nil {
		return nil, err
	}

	if !writeconcern.AckWrite(cmd.WriteConcern) {
		go func() {
			defer func() { _ = recover() }()
			defer conn.Close()

			_, _ = cmd.RoundTrip(ctx, desc, conn)
		}()

		return nil, command.ErrUnacknowledgedWrite
	}
	defer conn.Close()

	// If no explicit session and deployment supports sessions, start implicit session.
	if cmd.Session == nil && topo.SupportsSessions() {
		cmd.Session, err = session.NewClientSession(pool, clientID, session.Implicit)
		if err != nil {
			return nil, err
		}
		defer cmd.Session.EndSession()
	}

	return cmd.RoundTrip(ctx, desc, conn)
}

// Retryable writes are supported if the server supports sessions, the operation is not
// within a transaction, and the write is acknowledged
func retrySupported(
	topo *topology.Topology,
	desc description.SelectedServer,
	sess *session.Client,
	wc *writeconcern.WriteConcern,
) bool {
	return topo.SupportsSessions() &&
		description.SessionsSupported(desc.WireVersion) &&
		!(sess.TransactionInProgress() || sess.TransactionStarting()) &&
		writeconcern.AckWrite(wc)
}
