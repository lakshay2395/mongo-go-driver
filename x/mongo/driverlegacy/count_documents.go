// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package driverlegacy

import (
	"context"
	"time"

	"github.com/lakshay2395/mongo-go-driver/bson/bsoncodec"
	"github.com/lakshay2395/mongo-go-driver/mongo/options"
	"github.com/lakshay2395/mongo-go-driver/x/bsonx"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/session"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/topology"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/uuid"
	"github.com/lakshay2395/mongo-go-driver/x/network/command"
	"github.com/lakshay2395/mongo-go-driver/x/network/description"
)

// CountDocuments handles the full cycle dispatch and execution of a countDocuments command against the provided
// topology.
func CountDocuments(
	ctx context.Context,
	cmd command.CountDocuments,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
	registry *bsoncodec.Registry,
	opts ...*options.CountOptions,
) (int64, error) {

	if cmd.Session != nil && cmd.Session.PinnedServer != nil {
		selector = cmd.Session.PinnedServer
	}
	ss, err := topo.SelectServerLegacy(ctx, selector)
	if err != nil {
		return 0, err
	}

	desc := ss.Description()
	conn, err := ss.ConnectionLegacy(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	rp, err := getReadPrefBasedOnTransaction(cmd.ReadPref, cmd.Session)
	if err != nil {
		return 0, err
	}
	cmd.ReadPref = rp

	// If no explicit session and deployment supports sessions, start implicit session.
	if cmd.Session == nil && topo.SupportsSessions() {
		cmd.Session, err = session.NewClientSession(pool, clientID, session.Implicit)
		if err != nil {
			return 0, err
		}
		defer cmd.Session.EndSession()
	}

	countOpts := options.MergeCountOptions(opts...)

	// ignore Skip and Limit because we already have these options in the pipeline
	if countOpts.MaxTime != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{
			"maxTimeMS", bsonx.Int64(int64(*countOpts.MaxTime / time.Millisecond)),
		})
	}
	if countOpts.Collation != nil {
		if desc.WireVersion.Max < 5 {
			return 0, ErrCollation
		}
		collDoc, err := bsonx.ReadDoc(countOpts.Collation.ToDocument())
		if err != nil {
			return 0, err
		}
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"collation", bsonx.Document(collDoc)})
	}
	if countOpts.Hint != nil {
		hintElem, err := interfaceToElement("hint", countOpts.Hint, registry)
		if err != nil {
			return 0, err
		}

		cmd.Opts = append(cmd.Opts, hintElem)
	}

	return cmd.RoundTrip(ctx, desc, conn)
}
