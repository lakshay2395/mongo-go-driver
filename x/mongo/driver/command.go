package driver

import (
	"context"
	"errors"

	"github.com/lakshay2395/mongo-go-driver/mongo/readconcern"
	"github.com/lakshay2395/mongo-go-driver/mongo/readpref"
	"github.com/lakshay2395/mongo-go-driver/x/bsonx/bsoncore"
	"github.com/lakshay2395/mongo-go-driver/x/mongo/driverlegacy/session"
	"github.com/lakshay2395/mongo-go-driver/x/network/description"
)

// CommandOperation is used to run a generic operation against a server.
type CommandOperation struct {
	_ struct{} `drivergen:"-"`
	// Command sets the command that will be run.
	cmd bsoncore.Document `drivergen:"Command,constructorArg"`
	// ReadConcern sets the read concern to use when running the command.
	rc *readconcern.ReadConcern `drivergen:"ReadConcern,pointerExempt"`

	// Database sets the database to run the command against.
	database string
	// Deployment sets the Deployment to run the command against.
	d Deployment `drivergen:"Deployment"`

	selector description.ServerSelector `drivergen:"ServerSelector"`
	readPref *readpref.ReadPref         `drivergen:"ReadPreference,pointerExempt"`
	clock    *session.ClusterClock      `drivergen:"Clock,pointerExempt"`
	client   *session.Client            `drivergen:"Session,pointerExempt"`

	result bsoncore.Document `drivergen:"-"`
}

// Result returns the result of executing this operation.
//
// TODO(GODRIVER-617): This should be generated by drivergen.
func (co *CommandOperation) Result() bsoncore.Document { return co.result }

func (co *CommandOperation) processResponse(response bsoncore.Document, _ Server) error {
	co.result = response
	return nil
}

// TODO(GODRIVER-617): This should be generated by drivergen.
func (co *CommandOperation) command(dst []byte, _ description.SelectedServer) ([]byte, error) {
	return append(dst, co.cmd[4:len(co.cmd)-1]...), nil
}

// Execute runs this operations.
//
// TODO(GODRIVER-617): This should be generated by drivergen.
func (co *CommandOperation) Execute(ctx context.Context) error {
	if co.d == nil {
		return errors.New("a CommandOperation must have a Deployment set before Execute can be called")
	}

	if co.database == "" {
		return errors.New("Database must be of non-zero length")
	}
	return Operation{
		CommandFn:  co.command,
		Deployment: co.d,
		Database:   co.database,

		ProcessResponseFn: co.processResponse,

		Selector:       co.selector,
		ReadPreference: co.readPref,
		ReadConcern:    co.rc,

		Client: co.client,
		Clock:  co.clock,
	}.Execute(ctx, nil)
}
