// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//+build gssapi

package auth

import (
	"context"
	"testing"

	"github.com/lakshay2395/mongo-go-driver/x/network/address"
	"github.com/lakshay2395/mongo-go-driver/x/network/description"
)

func TestGSSAPIAuthenticator(t *testing.T) {
	t.Run("PropsError", func(t *testing.T) {
		// Cannot specify both CANONICALIZE_HOST_NAME and SERVICE_HOST

		authenticator := &GSSAPIAuthenticator{
			Username:    "foo",
			Password:    "bar",
			PasswordSet: true,
			Props: map[string]string{
				"CANONICALIZE_HOST_NAME": "true",
				"SERVICE_HOST":           "localhost",
			},
		}
		err := authenticator.Auth(context.Background(), description.Server{
			WireVersion: &description.VersionRange{
				Max: 6,
			},
			Addr: address.Address("foo:27017"),
		}, nil)
		if err == nil {
			t.Fatalf("expected err, got nil")
		}
	})

}
