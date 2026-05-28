package nexusoperation

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/protoutils"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIsWaitStageReached(t *testing.T) {
	t.Parallel()

	ctx := &chasm.MockContext{}
	allStatuses := protoutils.EnumValues[nexusoperationpb.OperationStatus]()

	tests := []struct {
		name       string
		waitStage  enumspb.NexusOperationWaitStage
		reached    []nexusoperationpb.OperationStatus
		notReached []nexusoperationpb.OperationStatus
	}{
		{
			name:       "Unspecified",
			waitStage:  enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED,
			notReached: allStatuses,
		},
		{
			name:      "Started",
			waitStage: enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
			reached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_STARTED,
				nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
				nexusoperationpb.OPERATION_STATUS_FAILED,
				nexusoperationpb.OPERATION_STATUS_CANCELED,
				nexusoperationpb.OPERATION_STATUS_TERMINATED,
				nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			},
			notReached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_UNSPECIFIED,
				nexusoperationpb.OPERATION_STATUS_SCHEDULED,
				nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			},
		},
		{
			name:      "Closed",
			waitStage: enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			reached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
				nexusoperationpb.OPERATION_STATUS_FAILED,
				nexusoperationpb.OPERATION_STATUS_CANCELED,
				nexusoperationpb.OPERATION_STATUS_TERMINATED,
				nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			},
			notReached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_UNSPECIFIED,
				nexusoperationpb.OPERATION_STATUS_SCHEDULED,
				nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
				nexusoperationpb.OPERATION_STATUS_STARTED,
			},
		},
	}

	coveredWaitStages := []enumspb.NexusOperationWaitStage{}
	for _, tt := range tests {
		coveredWaitStages = append(coveredWaitStages, tt.waitStage)
		t.Run(tt.name, func(t *testing.T) {
			op := newTestOperation()

			coveredStatuses := append(slices.Clone(tt.reached), tt.notReached...)
			require.ElementsMatch(t, allStatuses, coveredStatuses)

			for _, status := range tt.reached {
				op.Status = status
				require.Truef(t, op.isWaitStageReached(ctx, tt.waitStage), "expected %s to match %s", status, tt.waitStage)
			}

			for _, status := range tt.notReached {
				op.Status = status
				require.Falsef(t, op.isWaitStageReached(ctx, tt.waitStage), "expected %s not to match %s", status, tt.waitStage)
			}
		})
	}

	allWaitStages := protoutils.EnumValues[enumspb.NexusOperationWaitStage]()
	require.ElementsMatch(t, allWaitStages, coveredWaitStages)
}

func newScheduledTestOperation(t *testing.T, ctx *chasm.MockMutableContext) *Operation {
	t.Helper()
	op := newTestOperation()
	require.NoError(t, TransitionScheduled.Apply(op, ctx, EventScheduled{}))
	return op
}

func TestHandleNexusCompletion(t *testing.T) {
	newStartedOp := func(t *testing.T, ctx *chasm.MockMutableContext) *Operation {
		t.Helper()
		op := newScheduledTestOperation(t, ctx)
		require.NoError(t, TransitionStarted.Apply(op, ctx, EventStarted{OperationToken: "tok"}))
		return op
	}
	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceName(namespace.ID("ns-id")).Return(namespace.Name("ns-name"), nil).AnyTimes()

	newCtx := func() *chasm.MockMutableContext {
		return &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleNow: func(chasm.Component) time.Time { return defaultTime },
				HandleExecutionKey: func() chasm.ExecutionKey {
					return chasm.ExecutionKey{NamespaceID: "ns-id"}
				},
				HandleNamespaceEntry: func() *namespace.Namespace {
					return namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0)
				},
				GoCtx: context.WithValue(context.Background(), OperationContextKey, &OperationContext{
					MetricTagConfig: dynamicconfig.GetTypedPropertyFn(NexusMetricTagConfig{}),
				}),
			},
		}
	}

	t.Run("Success", func(t *testing.T) {
		t.Run("AfterStarted", func(t *testing.T) {
			ctx := newCtx()
			op := newStartedOp(t, ctx)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: op.GetRequestId(),
				Outcome: &persistencespb.ChasmNexusCompletion_Success{
					Success: mustToPayload(t, "result"),
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, op.GetStatus())
		})

		t.Run("CompletionBeforeStart", func(t *testing.T) {
			ctx := newCtx()
			op := newScheduledTestOperation(t, ctx)
			startTime := defaultTime.Add(-time.Second)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				StartTime:      timestamppb.New(startTime),
				RequestId:      op.GetRequestId(),
				OperationToken: "tok",
				Outcome: &persistencespb.ChasmNexusCompletion_Success{
					Success: mustToPayload(t, "result"),
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, op.GetStatus())
			require.Equal(t, "tok", op.GetOperationToken())
			require.Equal(t, startTime, op.GetStartedTime().AsTime())
		})

		t.Run("CompletionBeforeStartWithoutStartTime", func(t *testing.T) {
			ctx := newCtx()
			op := newScheduledTestOperation(t, ctx)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId:      op.GetRequestId(),
				OperationToken: "tok",
				Outcome: &persistencespb.ChasmNexusCompletion_Success{
					Success: mustToPayload(t, "result"),
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, op.GetStatus())
			require.Equal(t, "tok", op.GetOperationToken())
			require.Equal(t, defaultTime, op.GetStartedTime().AsTime())
		})
	})

	t.Run("Failure", func(t *testing.T) {
		t.Run("AfterStarted", func(t *testing.T) {
			ctx := newCtx()
			op := newStartedOp(t, ctx)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: op.GetRequestId(),
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{
					Failure: &failurepb.Failure{Message: "oops"},
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.GetStatus())
		})

		t.Run("CompletionBeforeStart", func(t *testing.T) {
			ctx := newCtx()
			op := newScheduledTestOperation(t, ctx)
			startTime := defaultTime.Add(-time.Second)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				StartTime:      timestamppb.New(startTime),
				RequestId:      op.GetRequestId(),
				OperationToken: "tok",
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{
					Failure: &failurepb.Failure{Message: "oops"},
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.GetStatus())
			require.Equal(t, "tok", op.GetOperationToken())
			require.Equal(t, startTime, op.GetStartedTime().AsTime())
		})
	})

	t.Run("Canceled", func(t *testing.T) {
		t.Run("AfterStarted", func(t *testing.T) {
			ctx := newCtx()
			op := newStartedOp(t, ctx)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: op.GetRequestId(),
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{
					Failure: &failurepb.Failure{
						Message: "canceled",
						FailureInfo: &failurepb.Failure_CanceledFailureInfo{
							CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
						},
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_CANCELED, op.GetStatus())
		})

		t.Run("CompletionBeforeStart", func(t *testing.T) {
			ctx := newCtx()
			op := newScheduledTestOperation(t, ctx)
			startTime := defaultTime.Add(-time.Second)
			err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				StartTime:      timestamppb.New(startTime),
				RequestId:      op.GetRequestId(),
				OperationToken: "tok",
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{
					Failure: &failurepb.Failure{
						Message: "canceled",
						FailureInfo: &failurepb.Failure_CanceledFailureInfo{
							CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
						},
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_CANCELED, op.GetStatus())
			require.Equal(t, "tok", op.GetOperationToken())
			require.Equal(t, startTime, op.GetStartedTime().AsTime())
		})
	})
}

func TestDescribeOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		status          nexusoperationpb.OperationStatus
		outcome         *nexusoperationpb.OperationOutcome // nil means no outcome set
		lastAttemptFail *failurepb.Failure
		expectedResult  *commonpb.Payload
		expectedFailure *failurepb.Failure
	}{
		{
			name:   "Successful",
			status: nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			outcome: &nexusoperationpb.OperationOutcome{
				Variant: &nexusoperationpb.OperationOutcome_Successful_{
					Successful: &nexusoperationpb.OperationOutcome_Successful{Result: payload.EncodeString("result")},
				},
			},
			expectedResult: payload.EncodeString("result"),
		},
		{
			name:   "Failed",
			status: nexusoperationpb.OPERATION_STATUS_FAILED,
			outcome: &nexusoperationpb.OperationOutcome{
				Variant: &nexusoperationpb.OperationOutcome_Failed_{
					Failed: &nexusoperationpb.OperationOutcome_Failed{
						Failure: &failurepb.Failure{Message: "outcome failure"},
					},
				},
			},
			expectedFailure: &failurepb.Failure{Message: "outcome failure"},
		},
		{
			name:            "NoOutcome_FallsBackToLastAttemptFailure",
			status:          nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			lastAttemptFail: &failurepb.Failure{Message: "last attempt failure"},
			expectedFailure: &failurepb.Failure{Message: "last attempt failure"},
		},
		{
			name:   "Outcome_PreferredOverLastAttemptFailure",
			status: nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			outcome: &nexusoperationpb.OperationOutcome{
				Variant: &nexusoperationpb.OperationOutcome_Failed_{
					Failed: &nexusoperationpb.OperationOutcome_Failed{
						Failure: &failurepb.Failure{Message: "operation timed out"},
					},
				},
			},
			lastAttemptFail: &failurepb.Failure{Message: "last attempt failure"},
			expectedFailure: &failurepb.Failure{Message: "operation timed out"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			op := NewOperation(&nexusoperationpb.OperationState{
				Status:             tc.status,
				LastAttemptFailure: tc.lastAttemptFail,
			})
			if tc.outcome != nil {
				op.Outcome = chasm.NewDataField(ctx, tc.outcome)
			}

			result, failure := op.outcome(ctx)
			protorequire.ProtoEqual(t, tc.expectedResult, result)
			protorequire.ProtoEqual(t, tc.expectedFailure, failure)
		})
	}
}

// TestNewStandaloneOperation_UserMetadataDualWrite verifies that user metadata
// supplied on a StartNexusOperationExecution request is persisted to BOTH the
// framework-level ChasmComponentAttributes (the authoritative new location)
// and the legacy OperationRequestData.user_metadata field. The dual-write is
// load-bearing for rollback safety: a binary rolled back to pre-migration code
// only knows how to read the legacy field, so dropping it would silently empty
// the user metadata on Describe for any operation created during the new-deploy
// window.
func TestNewStandaloneOperation_UserMetadataDualWrite(t *testing.T) {
	md := &sdkpb.UserMetadata{
		Summary: &commonpb.Payload{Data: []byte("summary-blob")},
		Details: &commonpb.Payload{Data: []byte("details-blob")},
	}

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 0) },
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{NamespaceID: "ns", BusinessID: "op", RunID: "run"}
			},
		},
	}

	op, err := newStandaloneOperation(ctx, &nexusoperationpb.StartNexusOperationRequest{
		NamespaceId: "ns",
		EndpointId:  "ep",
		FrontendRequest: &workflowservice.StartNexusOperationExecutionRequest{
			Namespace:    "ns",
			OperationId:  "op",
			Endpoint:     "ep",
			Service:      "svc",
			Operation:    "do-thing",
			UserMetadata: md,
		},
	})
	require.NoError(t, err)

	// New location: SetUserMetadata was recorded against the operation.
	require.Contains(t, ctx.UserMetadataByComponent, chasm.Component(op))
	require.Same(t, md, ctx.UserMetadataByComponent[op])

	// Legacy location: OperationRequestData also carries it so rolled-back code
	// can still surface user metadata via the old field.
	require.Same(t, md, op.RequestData.Get(ctx).GetUserMetadata()) //nolint:staticcheck // exercising legacy field
}

// TestAppendLinks_RejectsOverMaxLinksPerExecution verifies that the shared
// MaxLinksPerExecution cap is enforced for standalone Nexus operations:
// appending new links that would push the cumulative total over the cap fails
// and writes nothing to either the framework-level or legacy location.
func TestAppendLinks_RejectsOverMaxLinksPerExecution(t *testing.T) {
	const maxLinks = 2

	link := func(id string) *commonpb.Link {
		return &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: id,
				RunId:      "run",
			},
		}}
	}

	atCap := []*commonpb.Link{link("a"), link("b")}
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNamespaceEntry: func() *namespace.Namespace {
				return namespace.NewLocalNamespaceForTest(
					&persistencespb.NamespaceInfo{Name: "ns"},
					nil,
					"cluster",
				)
			},
			HandleLinks: func(chasm.Component) []*commonpb.Link { return atCap },
			GoCtx: context.WithValue(context.Background(), OperationContextKey, &OperationContext{
				MaxLinksPerExecution: func(string) int { return maxLinks },
				LinkMaxSize:          func(string) int { return 4000 },
			}),
		},
	}
	op := NewOperation(&nexusoperationpb.OperationState{
		RequestId: "req-id",
		Links:     atCap, //nolint:staticcheck // legacy mirror of framework-level state
	})

	err := op.appendLinks(ctx, []*commonpb.Link{link("c")})
	require.Error(t, err)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
	require.NotContains(t, ctx.LinksByRequest, chasm.Component(op))
	require.Equal(t, []*commonpb.Link{link("a"), link("b")}, op.Links) //nolint:staticcheck // legacy field unchanged after rejection
}

// TestAppendLinks_RejectsOversizedLink verifies that appendLinks rejects any
// individual link whose serialized size exceeds OperationContext.LinkMaxSize.
// This guards against an untrusted Nexus handler pushing oversized links past
// the cumulative cap one entry at a time.
func TestAppendLinks_RejectsOversizedLink(t *testing.T) {
	// 200-byte WorkflowId payload, intentionally larger than the test cap of 32 bytes.
	huge := strings.Repeat("x", 200)
	link := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: huge, RunId: "run"},
	}}

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNamespaceEntry: func() *namespace.Namespace {
				return namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns"}, nil, "cluster")
			},
			GoCtx: context.WithValue(context.Background(), OperationContextKey, &OperationContext{
				MaxLinksPerExecution: func(string) int { return 100 },
				LinkMaxSize:          func(string) int { return 32 },
			}),
		},
	}
	op := NewOperation(&nexusoperationpb.OperationState{RequestId: "req-id"})

	err := op.appendLinks(ctx, []*commonpb.Link{link})
	require.Error(t, err)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	require.NotContains(t, ctx.LinksByRequest, chasm.Component(op))
	require.Empty(t, op.Links) //nolint:staticcheck // legacy field unchanged on rejection
}

// TestAppendLinks_FailsWhenOperationContextMissing verifies that appendLinks
// returns an error (rather than silently skipping the per-execution cap) if
// the OperationContext was not registered as a chasm context value. The cap
// is load-bearing for DoS protection against unbounded link growth from
// callback senders, so a silent skip is unacceptable.
func TestAppendLinks_FailsWhenOperationContextMissing(t *testing.T) {
	link := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{
			Namespace:  "ns",
			WorkflowId: "wf",
			RunId:      "run",
		},
	}}

	ctx := &chasm.MockMutableContext{} // no OperationContext registered
	op := NewOperation(&nexusoperationpb.OperationState{RequestId: "req-id"})

	err := op.appendLinks(ctx, []*commonpb.Link{link})
	require.Error(t, err)
	require.NotContains(t, ctx.LinksByRequest, chasm.Component(op))
	require.Empty(t, op.Links) //nolint:staticcheck // legacy field unchanged on hard-fail
}

// TestAppendLinks_MergesPerRequestEntry verifies that two successive
// appendLinks calls with the same OperationState.RequestId extend the
// per-request entry rather than overwriting it; this catches the regression
// where appendLinks used to collapse all entries under one key.
func TestAppendLinks_MergesPerRequestEntry(t *testing.T) {
	linkA := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "a", RunId: "run"},
	}}
	linkB := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "b", RunId: "run"},
	}}

	// First call writes [linkA] under "req-id"; HandleRequestLinks then reflects that prior write
	// so the second call's read-modify-write merges [linkA, linkB] rather than overwriting.
	stored := map[string][]*commonpb.Link{}
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNamespaceEntry: func() *namespace.Namespace {
				return namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns"}, nil, "cluster")
			},
			HandleRequestLinks: func(_ chasm.Component, reqID string) ([]*commonpb.Link, error) {
				return stored[reqID], nil
			},
			GoCtx: context.WithValue(context.Background(), OperationContextKey, &OperationContext{
				MaxLinksPerExecution: func(string) int { return 100 },
				LinkMaxSize:          func(string) int { return 4000 },
			}),
		},
	}
	op := NewOperation(&nexusoperationpb.OperationState{RequestId: "req-id"})

	require.NoError(t, op.appendLinks(ctx, []*commonpb.Link{linkA}))
	stored["req-id"] = ctx.LinksByRequest[op]["req-id"]
	require.NoError(t, op.appendLinks(ctx, []*commonpb.Link{linkB}))

	got := ctx.LinksByRequest[op]["req-id"]
	require.Len(t, got, 2)
	require.Same(t, linkA, got[0])
	require.Same(t, linkB, got[1])
}

// TestAppendLinks_DualWrite verifies that appendLinks records new links in
// BOTH the framework-level ChasmComponentAttributes.requests (keyed by the
// operation's request ID) and the legacy OperationState.links field.
func TestAppendLinks_DualWrite(t *testing.T) {
	link := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{
			Namespace:  "ns",
			WorkflowId: "wf",
			RunId:      "run",
		},
	}}

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNamespaceEntry: func() *namespace.Namespace {
				return namespace.NewLocalNamespaceForTest(
					&persistencespb.NamespaceInfo{Name: "ns"},
					nil,
					"cluster",
				)
			},
			GoCtx: context.WithValue(context.Background(), OperationContextKey, &OperationContext{
				MaxLinksPerExecution: func(string) int { return 100 },
				LinkMaxSize:          func(string) int { return 4000 },
			}),
		},
	}
	op := NewOperation(&nexusoperationpb.OperationState{RequestId: "req-id"})

	require.NoError(t, op.appendLinks(ctx, []*commonpb.Link{link}))

	// New location: keyed by the operation's request ID.
	require.Contains(t, ctx.LinksByRequest, chasm.Component(op))
	require.Equal(t, []*commonpb.Link{link}, ctx.LinksByRequest[op]["req-id"])

	// Legacy location: OperationState.Links carries the same link for rolled-back readers.
	require.Equal(t, []*commonpb.Link{link}, op.Links) //nolint:staticcheck // exercising legacy field
}

// TestEffectiveLinks_FallsBackToLegacy ensures that operations persisted before
// the migration (no ChasmComponentAttributes.requests; only the legacy
// OperationState.links populated) still surface their links to readers.
func TestEffectiveLinks_FallsBackToLegacy(t *testing.T) {
	legacyLink := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{
			Namespace:  "ns",
			WorkflowId: "wf",
			RunId:      "run",
		},
	}}

	ctx := &chasm.MockContext{} // HandleLinks nil → returns nil, mimicking absent new field.
	op := NewOperation(&nexusoperationpb.OperationState{
		Links: []*commonpb.Link{legacyLink}, //nolint:staticcheck // exercising legacy field
	})

	got := op.effectiveLinks(ctx)
	require.Equal(t, []*commonpb.Link{legacyLink}, got)
}

// TestEffectiveUserMetadata_NexusOp_FallsBackToLegacy ensures the same fallback
// for user metadata on standalone nexus operations.
func TestEffectiveUserMetadata_NexusOp_FallsBackToLegacy(t *testing.T) {
	legacyMD := &sdkpb.UserMetadata{Summary: &commonpb.Payload{Data: []byte("legacy")}}

	ctx := &chasm.MockMutableContext{} // HandleUserMetadata nil → returns nil.
	op := NewOperation(&nexusoperationpb.OperationState{})
	op.RequestData = chasm.NewDataField(ctx, &nexusoperationpb.OperationRequestData{
		UserMetadata: legacyMD, //nolint:staticcheck // exercising legacy field
	})

	got := op.effectiveUserMetadata(ctx)
	require.Same(t, legacyMD, got)
}
