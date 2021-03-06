package driver

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/lakshay2395/mongo-go-driver/bson/bsontype"
	"github.com/lakshay2395/mongo-go-driver/bson/primitive"
	"github.com/lakshay2395/mongo-go-driver/mongo/readconcern"
	"github.com/lakshay2395/mongo-go-driver/mongo/readpref"
	"github.com/lakshay2395/mongo-go-driver/mongo/writeconcern"
	"github.com/lakshay2395/mongo-go-driver/x/bsonx/bsoncore"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/session"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/uuid"
	"github.com/lakshay2395/mongo-go-driver/x/network/address"
	"github.com/lakshay2395/mongo-go-driver/x/network/description"
	"github.com/lakshay2395/mongo-go-driver/x/network/wiremessage"
)

func noerr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		t.FailNow()
	}
}

func compareErrors(err1, err2 error) bool {
	if err1 == nil && err2 == nil {
		return true
	}

	if err1 == nil || err2 == nil {
		return false
	}

	if err1.Error() != err2.Error() {
		return false
	}

	return true
}

func TestOperation(t *testing.T) {
	t.Run("selectServer", func(t *testing.T) {
		t.Run("returns validation error", func(t *testing.T) {
			op := &Operation{}
			_, err := op.selectServer(context.Background())
			if err == nil {
				t.Error("Expected a validation error from selectServer, but got <nil>")
			}
		})
		t.Run("returns context error when expired", func(t *testing.T) {
			op := &Operation{
				CommandFn:  func([]byte, description.SelectedServer) ([]byte, error) { return nil, nil },
				Deployment: new(mockDeployment),
				Database:   "testing",
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			want := context.Canceled
			_, got := op.selectServer(ctx)
			if got != want {
				t.Errorf("Did not get expected error. got %v; want %v", got, want)
			}
		})
		t.Run("uses specified server selector", func(t *testing.T) {
			want := new(mockServerSelector)
			d := new(mockDeployment)
			op := &Operation{
				CommandFn:  func([]byte, description.SelectedServer) ([]byte, error) { return nil, nil },
				Deployment: d,
				Database:   "testing",
				Selector:   want,
			}
			_, err := op.selectServer(context.Background())
			noerr(t, err)
			got := d.params.selector
			if !cmp.Equal(got, want) {
				t.Errorf("Did not get expected server selector. got %v; want %v", got, want)
			}
		})
		t.Run("uses a default server selector", func(t *testing.T) {
			d := new(mockDeployment)
			op := &Operation{
				CommandFn:  func([]byte, description.SelectedServer) ([]byte, error) { return nil, nil },
				Deployment: d,
				Database:   "testing",
			}
			_, err := op.selectServer(context.Background())
			noerr(t, err)
			if d.params.selector == nil {
				t.Error("The selectServer method should use a default selector when not specified on Operation, but it passed <nil>.")
			}
		})
	})
	t.Run("Validate", func(t *testing.T) {
		cmdFn := func([]byte, description.SelectedServer) ([]byte, error) { return nil, nil }
		d := new(mockDeployment)
		testCases := []struct {
			name string
			op   *Operation
			err  error
		}{
			{"CommandFn", &Operation{}, InvalidOperationError{MissingField: "CommandFn"}},
			{"Deployment", &Operation{CommandFn: cmdFn}, InvalidOperationError{MissingField: "Deployment"}},
			{"Database", &Operation{CommandFn: cmdFn, Deployment: d}, InvalidOperationError{MissingField: "Database"}},
			{"<nil>", &Operation{CommandFn: cmdFn, Deployment: d, Database: "test"}, nil},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				if tc.op == nil {
					t.Fatal("op cannot be <nil>")
				}
				want := tc.err
				got := tc.op.Validate()
				if !cmp.Equal(got, want, cmp.Comparer(compareErrors)) {
					t.Errorf("Did not validate properly. got %v; want %v", got, want)
				}
			})
		}
	})
	t.Run("retryable", func(t *testing.T) {
		deploymentRetry := new(mockDeployment)
		deploymentRetry.returns.retry = true

		deploymentNoRetry := new(mockDeployment)

		sessPool := session.NewPool(nil)
		id, err := uuid.New()
		noerr(t, err)

		sess, err := session.NewClientSession(sessPool, id, session.Explicit)
		noerr(t, err)

		sessStartingTransaction, err := session.NewClientSession(sessPool, id, session.Explicit)
		noerr(t, err)
		err = sessStartingTransaction.StartTransaction(nil)
		noerr(t, err)

		sessInProgressTransaction, err := session.NewClientSession(sessPool, id, session.Explicit)
		noerr(t, err)
		err = sessInProgressTransaction.StartTransaction(nil)
		noerr(t, err)
		sessInProgressTransaction.ApplyCommand(description.Server{})

		wcAck := writeconcern.New(writeconcern.WMajority())
		wcUnack := writeconcern.New(writeconcern.W(0))

		descRetryable := description.Server{WireVersion: &description.VersionRange{Min: 0, Max: 7}}
		descNotRetryable := description.Server{WireVersion: &description.VersionRange{Min: 0, Max: 5}}

		testCases := []struct {
			name string
			op   Operation
			desc description.Server
			want RetryType
		}{
			{"deployment doesn't support", Operation{Deployment: deploymentNoRetry}, description.Server{}, RetryType(0)},
			{"wire version too low", Operation{Deployment: deploymentRetry, Client: sess, WriteConcern: wcAck}, descNotRetryable, RetryType(0)},
			{
				"transaction in progress",
				Operation{Deployment: deploymentRetry, Client: sessInProgressTransaction, WriteConcern: wcAck},
				descRetryable, RetryType(0),
			},
			{
				"transaction starting",
				Operation{Deployment: deploymentRetry, Client: sessStartingTransaction, WriteConcern: wcAck},
				descRetryable, RetryType(0),
			},
			{"unacknowledged write concern", Operation{Deployment: deploymentRetry, Client: sess, WriteConcern: wcUnack}, descRetryable, RetryType(0)},
			{
				"acknowledged write concern",
				Operation{Deployment: deploymentRetry, Client: sess, WriteConcern: wcAck, RetryType: RetryWrite},
				descRetryable, RetryWrite,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				got := tc.op.retryable(tc.desc)
				if got != tc.want {
					t.Errorf("Did not receive expected RetryType. got %v; want %v", got, tc.want)
				}
			})
		}
	})
	t.Run("roundTrip", func(t *testing.T) {
		testCases := []struct {
			name    string
			conn    *mockConnection
			paramWM []byte // parameter wire message
			wantWM  []byte // wire message that should be returned
			wantErr error  // error that should be returned
		}{
			{
				"returns write error",
				&mockConnection{rWriteErr: errors.New("write error")},
				nil, nil,
				Error{Message: "write error", Labels: []string{TransientTransactionError, NetworkError}},
			},
			{
				"returns read error",
				&mockConnection{rReadErr: errors.New("read error")},
				nil, nil,
				Error{Message: "read error", Labels: []string{TransientTransactionError, NetworkError}},
			},
			{"success", &mockConnection{rReadWM: []byte{0x01, 0x02, 0x03, 0x04}}, nil, []byte{0x01, 0x02, 0x03, 0x04}, nil},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				gotWM, gotErr := Operation{}.roundTrip(context.Background(), tc.conn, tc.paramWM)
				if !bytes.Equal(gotWM, tc.wantWM) {
					t.Errorf("Returned wire messages are not equal. got %v; want %v", gotWM, tc.wantWM)
				}
				if !cmp.Equal(gotErr, tc.wantErr, cmp.Comparer(compareErrors)) {
					t.Errorf("Returned error is not equal to expected error. got %v; want %v", gotErr, tc.wantErr)
				}
			})
		}
	})
	t.Run("addReadConcern", func(t *testing.T) {
		want := bsoncore.AppendDocumentElement(nil, "readConcern", bsoncore.BuildDocument(nil,
			bsoncore.AppendStringElement(nil, "level", "majority"),
		))
		got, err := Operation{ReadConcern: readconcern.Majority()}.addReadConcern(nil, description.SelectedServer{})
		noerr(t, err)
		if !bytes.Equal(got, want) {
			t.Errorf("ReadConcern elements do not match. got %v; want %v", got, want)
		}
	})
	t.Run("addWriteConcern", func(t *testing.T) {
		want := bsoncore.AppendDocumentElement(nil, "writeConcern", bsoncore.BuildDocumentFromElements(
			nil, bsoncore.AppendStringElement(nil, "w", "majority"),
		))
		got, err := Operation{WriteConcern: writeconcern.New(writeconcern.WMajority())}.addWriteConcern(nil)
		noerr(t, err)
		if !bytes.Equal(got, want) {
			t.Errorf("WriteConcern elements do not match. got %v; want %v", got, want)
		}
	})
	t.Run("addSession", func(t *testing.T) { t.Skip("These tests should be covered by spec tests.") })
	t.Run("addClusterTime", func(t *testing.T) {
		t.Run("adds max cluster time", func(t *testing.T) {
			want := bsoncore.AppendDocumentElement(nil, "$clusterTime", bsoncore.BuildDocumentFromElements(nil,
				bsoncore.AppendTimestampElement(nil, "clusterTime", 1234, 5678),
			))
			newer := bsoncore.BuildDocumentFromElements(nil, want)
			older := bsoncore.BuildDocumentFromElements(nil,
				bsoncore.AppendDocumentElement(nil, "$clusterTime", bsoncore.BuildDocumentFromElements(nil,
					bsoncore.AppendTimestampElement(nil, "clusterTime", 1234, 5670),
				)),
			)

			clusterClock := new(session.ClusterClock)
			clusterClock.AdvanceClusterTime(newer)
			sessPool := session.NewPool(nil)
			id, err := uuid.New()
			noerr(t, err)

			sess, err := session.NewClientSession(sessPool, id, session.Explicit)
			noerr(t, err)
			err = sess.AdvanceClusterTime(older)
			noerr(t, err)

			got := Operation{Client: sess, Clock: clusterClock}.addClusterTime(nil, description.SelectedServer{
				Server: description.Server{WireVersion: &description.VersionRange{Min: 0, Max: 7}},
			})
			if !bytes.Equal(got, want) {
				t.Errorf("ClusterTimes do not match. got %v; want %v", got, want)
			}
		})
	})
	t.Run("updateClusterTimes", func(t *testing.T) {
		clustertime := bsoncore.BuildDocumentFromElements(nil,
			bsoncore.AppendDocumentElement(nil, "$clusterTime", bsoncore.BuildDocumentFromElements(nil,
				bsoncore.AppendTimestampElement(nil, "clusterTime", 1234, 5678),
			)),
		)

		clusterClock := new(session.ClusterClock)
		sessPool := session.NewPool(nil)
		id, err := uuid.New()
		noerr(t, err)

		sess, err := session.NewClientSession(sessPool, id, session.Explicit)
		noerr(t, err)
		Operation{Client: sess, Clock: clusterClock}.updateClusterTimes(clustertime)

		got := sess.ClusterTime
		if !bytes.Equal(got, clustertime) {
			t.Errorf("ClusterTimes do not match. got %v; want %v", got, clustertime)
		}
		got = clusterClock.GetClusterTime()
		if !bytes.Equal(got, clustertime) {
			t.Errorf("ClusterTimes do not match. got %v; want %v", got, clustertime)
		}

		Operation{}.updateClusterTimes(bsoncore.BuildDocumentFromElements(nil)) // should do nothing
	})
	t.Run("updateOperationTime", func(t *testing.T) {
		want := primitive.Timestamp{T: 1234, I: 4567}

		sessPool := session.NewPool(nil)
		id, err := uuid.New()
		noerr(t, err)

		sess, err := session.NewClientSession(sessPool, id, session.Explicit)
		noerr(t, err)
		if sess.OperationTime != nil {
			t.Fatal("OperationTime should not be set on new session.")
		}
		response := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendTimestampElement(nil, "operationTime", want.T, want.I))
		Operation{Client: sess}.updateOperationTime(response)
		got := sess.OperationTime
		if got.T != want.T || got.I != want.I {
			t.Errorf("OperationTimes do not match. got %v; want %v", got, want)
		}

		response = bsoncore.BuildDocumentFromElements(nil)
		Operation{Client: sess}.updateOperationTime(response)
		got = sess.OperationTime
		if got.T != want.T || got.I != want.I {
			t.Errorf("OperationTimes do not match. got %v; want %v", got, want)
		}

		Operation{}.updateOperationTime(response) // should do nothing
	})
	t.Run("createReadPref", func(t *testing.T) {
		rpWithTags := bsoncore.BuildDocumentFromElements(nil,
			bsoncore.AppendStringElement(nil, "mode", "secondaryPreferred"),
			bsoncore.BuildArrayElement(nil, "tags",
				bsoncore.Value{Type: bsontype.EmbeddedDocument,
					Data: bsoncore.BuildDocumentFromElements(nil,
						bsoncore.AppendStringElement(nil, "disk", "ssd"),
						bsoncore.AppendStringElement(nil, "use", "reporting"),
					),
				},
			),
		)
		rpWithMaxStaleness := bsoncore.BuildDocumentFromElements(nil,
			bsoncore.AppendStringElement(nil, "mode", "secondaryPreferred"),
			bsoncore.AppendInt32Element(nil, "maxStalenessSeconds", 25),
		)

		rpPrimaryPreferred := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendStringElement(nil, "mode", "primaryPreferred"))
		rpPrimary := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendStringElement(nil, "mode", "primary"))
		rpSecondaryPreferred := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendStringElement(nil, "mode", "secondaryPreferred"))
		rpSecondary := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendStringElement(nil, "mode", "secondary"))
		rpNearest := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendStringElement(nil, "mode", "nearest"))

		testCases := []struct {
			name       string
			rp         *readpref.ReadPref
			serverKind description.ServerKind
			topoKind   description.TopologyKind
			opQuery    bool
			want       bsoncore.Document
		}{
			{"nil/single/mongos", nil, description.Mongos, description.Single, false, nil},
			{"nil/single/secondary", nil, description.RSSecondary, description.Single, false, rpPrimaryPreferred},
			{"primary/mongos", readpref.Primary(), description.Mongos, description.Sharded, false, nil},
			{"primary/single", readpref.Primary(), description.RSPrimary, description.Single, false, rpPrimaryPreferred},
			{"primary/primary", readpref.Primary(), description.RSPrimary, description.ReplicaSet, false, rpPrimary},
			{"primaryPreferred", readpref.PrimaryPreferred(), description.RSSecondary, description.ReplicaSet, false, rpPrimaryPreferred},
			{"secondaryPreferred/mongos/opquery", readpref.SecondaryPreferred(), description.Mongos, description.Sharded, true, nil},
			{"secondaryPreferred", readpref.SecondaryPreferred(), description.RSSecondary, description.ReplicaSet, false, rpSecondaryPreferred},
			{"secondary", readpref.Secondary(), description.RSSecondary, description.ReplicaSet, false, rpSecondary},
			{"nearest", readpref.Nearest(), description.RSSecondary, description.ReplicaSet, false, rpNearest},
			{
				"secondaryPreferred/withTags",
				readpref.SecondaryPreferred(readpref.WithTags("disk", "ssd", "use", "reporting")),
				description.RSSecondary, description.ReplicaSet, false, rpWithTags,
			},
			{
				"secondaryPreferred/withMaxStaleness",
				readpref.SecondaryPreferred(readpref.WithMaxStaleness(25 * time.Second)),
				description.RSSecondary, description.ReplicaSet, false, rpWithMaxStaleness,
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				got := Operation{ReadPreference: tc.rp}.createReadPref(tc.serverKind, tc.topoKind, tc.opQuery)
				if !bytes.Equal(got, tc.want) {
					t.Errorf("Returned documents do not match. got %v; want %v", got, tc.want)
				}
			})
		}
	})
	t.Run("slaveOK", func(t *testing.T) {
		t.Run("description.SelectedServer", func(t *testing.T) {
			want := wiremessage.SlaveOK
			desc := description.SelectedServer{
				Kind:   description.Single,
				Server: description.Server{Kind: description.RSSecondary},
			}
			got := Operation{}.slaveOK(desc)
			if got != want {
				t.Errorf("Did not receive expected query flags. got %v; want %v", got, want)
			}
		})
		t.Run("readPreference", func(t *testing.T) {
			want := wiremessage.SlaveOK
			got := Operation{ReadPreference: readpref.Secondary()}.slaveOK(description.SelectedServer{})
			if got != want {
				t.Errorf("Did not receive expected query flags. got %v; want %v", got, want)
			}
		})
		t.Run("not slaveOK", func(t *testing.T) {
			var want wiremessage.QueryFlag
			got := Operation{}.slaveOK(description.SelectedServer{})
			if got != want {
				t.Errorf("Did not receive expected query flags. got %v; want %v", got, want)
			}
		})
	})
}

type mockDeployment struct {
	params struct {
		selector description.ServerSelector
	}
	returns struct {
		server Server
		err    error
		retry  bool
		kind   description.TopologyKind
	}
}

func (m *mockDeployment) SelectServer(ctx context.Context, desc description.ServerSelector) (Server, error) {
	m.params.selector = desc
	return m.returns.server, m.returns.err
}

func (m *mockDeployment) SupportsRetry() bool            { return m.returns.retry }
func (m *mockDeployment) Kind() description.TopologyKind { return m.returns.kind }

type mockServerSelector struct{}

func (m *mockServerSelector) SelectServer(description.Topology, []description.Server) ([]description.Server, error) {
	panic("not implemented")
}

type mockConnection struct {
	// parameters
	pWriteWM []byte
	pReadDst []byte

	// returns
	rWriteErr error
	rReadWM   []byte
	rReadErr  error
	rDesc     description.Server
	rCloseErr error
	rID       string
	rAddr     address.Address
}

func (m *mockConnection) Description() description.Server { return m.rDesc }
func (m *mockConnection) Close() error                    { return m.rCloseErr }
func (m *mockConnection) ID() string                      { return m.rID }
func (m *mockConnection) Address() address.Address        { return m.rAddr }

func (m *mockConnection) WriteWireMessage(_ context.Context, wm []byte) error {
	m.pWriteWM = wm
	return m.rWriteErr
}

func (m *mockConnection) ReadWireMessage(_ context.Context, dst []byte) ([]byte, error) {
	m.pReadDst = dst
	return m.rReadWM, m.rReadErr
}
