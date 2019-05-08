// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package driverlegacy

import (
	"context"

	"github.com/lakshay2395/mongo-go-driver/bson"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/session"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/topology"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/uuid"
	"github.com/lakshay2395/mongo-go-driver/x/network/command"
	"github.com/lakshay2395/mongo-go-driver/x/network/description"
)

// DropCollection handles the full cycle dispatch and execution of a dropCollection
// command against the provided topology.
func DropCollection(
	ctx context.Context,
	cmd command.DropCollection,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
) (bson.Raw, error) {

	ss, err := topo.SelectServerLegacy(ctx, selector)
	if err != nil {
		return nil, err
	}

	conn, err := ss.ConnectionLegacy(ctx)
	if err != nil {
		return nil, err
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

	return cmd.RoundTrip(ctx, ss.Description(), conn)
}
