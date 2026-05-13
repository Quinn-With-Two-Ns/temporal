package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
)

// ContextMetadataTrailerTestSuite is an end-to-end reproduction of a regression
// in common/rpc/interceptor/context_metadata_interceptor.go.
//
// MutableStateImpl.SetContextMetadata pushes the raw workflow type, task-queue
// name, and (on heartbeat) activity type / activity task-queue into context
// metadata. ContextMetadataInterceptor then copies all of those values into
// gRPC response trailers via grpc.SetTrailer. HTTP/2 header values forbid the
// C0 control bytes (0x00-0x1F except HTAB 0x09) and DEL (0x7F), so any name
// containing such a byte makes the framer reject the response with
//
//	invalid header field value for "workflow-type"
//
// which the frontend masks as
//
//	Internal: something went wrong, please retry (<hash>)
//
// The SDK retries on Internal until the call deadline elapses, so every RPC
// touching such a workflow / activity effectively never succeeds.
//
// User impact: any workflow whose type name (or task-queue name, or activity
// type / activity task-queue name) contains a control character cannot be
// operated on through the normal client API. The error surface is opaque
// (masked Internal), which makes this hard to diagnose.
type ContextMetadataTrailerTestSuite struct {
	testcore.FunctionalTestBase
}

func TestContextMetadataTrailerTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ContextMetadataTrailerTestSuite))
}

func (s *ContextMetadataTrailerTestSuite) sdkClient() sdkclient.Client {
	c, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.Require().NoError(err)
	return c
}

// runWithWorkflowType registers and starts a workflow whose registered name is
// workflowType, then waits for the result. Returns the error observed by the
// SDK caller.
func (s *ContextMetadataTrailerTestSuite) runWithWorkflowType(workflowType string) error {
	c := s.sdkClient()
	defer c.Close()

	tq := "tq-" + uuid.NewString()
	w := worker.New(c, tq, worker.Options{})
	w.RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: workflowType},
	)
	s.Require().NoError(w.Start())
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	run, err := c.ExecuteWorkflow(ctx,
		sdkclient.StartWorkflowOptions{
			ID:        "wf-" + uuid.NewString(),
			TaskQueue: tq,
		},
		workflowType,
	)
	if err != nil {
		return err
	}
	return run.Get(ctx, nil)
}

func (s *ContextMetadataTrailerTestSuite) TestWorkflowType_PlainASCII_Succeeds() {
	// Sanity check that the wiring works for ordinary type names.
	s.Require().NoError(s.runWithWorkflowType("PlainAsciiWorkflowType"))
}

func (s *ContextMetadataTrailerTestSuite) TestWorkflowType_WithNewline_FailsEndToEnd() {
	// "\n" is illegal in HTTP/2 header values, so SetTrailer with
	// workflow-type="Foo\nBar" causes the server response to fail to encode.
	err := s.runWithWorkflowType("Foo\nBar")
	s.Require().Error(err,
		"expected end-to-end failure for workflow type containing a newline (context_metadata_interceptor SetTrailer)")
}

func (s *ContextMetadataTrailerTestSuite) TestWorkflowType_WithCarriageReturn_FailsEndToEnd() {
	err := s.runWithWorkflowType("Foo\rBar")
	s.Require().Error(err,
		"expected end-to-end failure for workflow type containing CR")
}

func (s *ContextMetadataTrailerTestSuite) TestWorkflowType_WithNUL_FailsEndToEnd() {
	err := s.runWithWorkflowType("Foo\x00Bar")
	s.Require().Error(err,
		"expected end-to-end failure for workflow type containing a NUL")
}

func (s *ContextMetadataTrailerTestSuite) TestWorkflowType_WithUTF8_Succeeds() {
	// Non-ASCII (high-bit) bytes are NOT HTTP/2 controls, so UTF-8 names
	// pass the framer and round-trip successfully. Kept as a regression
	// guard so the bug fix doesn't accidentally break legitimate names.
	s.Require().NoError(s.runWithWorkflowType("Workflow-日本語"))
}
