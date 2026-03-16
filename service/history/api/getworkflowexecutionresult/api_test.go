package getworkflowexecutionresult

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	getWorkflowExecutionResultSuite struct {
		suite.Suite
		*require.Assertions

		controller        *gomock.Controller
		shardContext      *historyi.MockShardContext
		namespaceRegistry *namespace.MockRegistry

		workflowCache              *wcache.MockCache
		workflowConsistencyChecker api.WorkflowConsistencyChecker

		currentContext      *historyi.MockWorkflowContext
		currentMutableState *historyi.MockMutableState
	}
)

func TestGetWorkflowExecutionResultSuite(t *testing.T) {
	s := new(getWorkflowExecutionResultSuite)
	suite.Run(t, s)
}

func (s *getWorkflowExecutionResultSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.namespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.namespaceRegistry.EXPECT().GetNamespaceByID(tests.GlobalNamespaceEntry.ID()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	s.shardContext = historyi.NewMockShardContext(s.controller)
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry).AnyTimes()
	s.shardContext.EXPECT().GetClusterMetadata().Return(clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true))).AnyTimes()
	s.shardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()

	s.currentMutableState = historyi.NewMockMutableState(s.controller)
	s.currentMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.currentContext = historyi.NewMockWorkflowContext(s.controller)
	s.currentContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.currentMutableState, nil).AnyTimes()

	s.workflowCache = wcache.NewMockCache(s.controller)
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), chasm.WorkflowArchetypeID, locks.PriorityHigh).
		Return(s.currentContext, wcache.NoopReleaseFn, nil).AnyTimes()

	s.workflowConsistencyChecker = api.NewWorkflowConsistencyChecker(
		s.shardContext,
		s.workflowCache,
	)
}

func (s *getWorkflowExecutionResultSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *getWorkflowExecutionResultSuite) newRequest() *historyservice.GetWorkflowExecutionResultRequest {
	return &historyservice.GetWorkflowExecutionResultRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.GetWorkflowExecutionResultRequest{
			Namespace: tests.Namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: tests.WorkflowID,
				RunId:      tests.RunID,
			},
		},
	}
}

func (s *getWorkflowExecutionResultSuite) TestCompletedWorkflow() {
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}).AnyTimes()
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	expectedResult := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{{Data: []byte("test-result")}},
	}
	s.currentMutableState.EXPECT().GetCompletionEvent(gomock.Any()).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		EventId:   10,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				Result: expectedResult,
			},
		},
	}, nil)

	resp, err := Invoke(context.Background(), s.newRequest(), s.shardContext, s.workflowConsistencyChecker)
	s.NoError(err)
	s.NotNil(resp)

	completed := resp.GetResponse().GetCompleted()
	s.NotNil(completed)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, completed.GetStatus())
	s.Equal(tests.WorkflowID, completed.GetExecution().GetWorkflowId())
	s.Equal(tests.RunID, completed.GetExecution().GetRunId())
	s.Equal(expectedResult, completed.GetResult())
	s.Nil(completed.GetFailure())
	// Verify link to completion event.
	s.Len(resp.GetResponse().GetLinks(), 1)
}

func (s *getWorkflowExecutionResultSuite) TestFailedWorkflow() {
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	}).AnyTimes()
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	expectedFailure := &failurepb.Failure{Message: "test failure"}
	s.currentMutableState.EXPECT().GetCompletionEvent(gomock.Any()).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		EventId:   10,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				Failure: expectedFailure,
			},
		},
	}, nil)

	resp, err := Invoke(context.Background(), s.newRequest(), s.shardContext, s.workflowConsistencyChecker)
	s.NoError(err)

	completed := resp.GetResponse().GetCompleted()
	s.NotNil(completed)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, completed.GetStatus())
	s.Equal(expectedFailure, completed.GetFailure())
	s.Nil(completed.GetResult())
}

func (s *getWorkflowExecutionResultSuite) TestCanceledWorkflow() {
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	}).AnyTimes()
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	cancelDetails := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{{Data: []byte("cancel-details")}},
	}
	s.currentMutableState.EXPECT().GetCompletionEvent(gomock.Any()).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		EventId:   10,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
			WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{
				Details: cancelDetails,
			},
		},
	}, nil)

	resp, err := Invoke(context.Background(), s.newRequest(), s.shardContext, s.workflowConsistencyChecker)
	s.NoError(err)

	completed := resp.GetResponse().GetCompleted()
	s.NotNil(completed)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, completed.GetStatus())
	s.NotNil(completed.GetFailure())
	s.NotNil(completed.GetFailure().GetCanceledFailureInfo())
}

func (s *getWorkflowExecutionResultSuite) TestTerminatedWorkflow() {
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	}).AnyTimes()
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	s.currentMutableState.EXPECT().GetCompletionEvent(gomock.Any()).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		EventId:   10,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{},
		},
	}, nil)

	resp, err := Invoke(context.Background(), s.newRequest(), s.shardContext, s.workflowConsistencyChecker)
	s.NoError(err)

	completed := resp.GetResponse().GetCompleted()
	s.NotNil(completed)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, completed.GetStatus())
	s.NotNil(completed.GetFailure())
	s.NotNil(completed.GetFailure().GetTerminatedFailureInfo())
}

func (s *getWorkflowExecutionResultSuite) TestTimedOutWorkflow() {
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}).AnyTimes()
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	s.currentMutableState.EXPECT().GetCompletionEvent(gomock.Any()).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		EventId:   10,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
			WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{},
		},
	}, nil)

	resp, err := Invoke(context.Background(), s.newRequest(), s.shardContext, s.workflowConsistencyChecker)
	s.NoError(err)

	completed := resp.GetResponse().GetCompleted()
	s.NotNil(completed)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, completed.GetStatus())
	s.NotNil(completed.GetFailure())
	s.NotNil(completed.GetFailure().GetTimeoutFailureInfo())
}

func (s *getWorkflowExecutionResultSuite) TestRunningWorkflowNoCallbacks() {
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}).AnyTimes()
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)

	resp, err := Invoke(context.Background(), s.newRequest(), s.shardContext, s.workflowConsistencyChecker)
	s.NoError(err)
	s.NotNil(resp)

	notCompleted := resp.GetResponse().GetNotCompleted()
	s.NotNil(notCompleted)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, notCompleted.GetStatus())
	s.Equal(tests.WorkflowID, notCompleted.GetExecution().GetWorkflowId())
	s.Equal(tests.RunID, notCompleted.GetExecution().GetRunId())
}

func (s *getWorkflowExecutionResultSuite) TestRunningWorkflowWithCallbacks() {
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}).AnyTimes()
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)

	callbacks := []*commonpb.Callback{
		{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://example.com/callback",
				},
			},
		},
	}

	s.currentMutableState.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(
		nil,     // versioningOverride
		false,   // unsetVersioningOverride
		"test-request-id",
		callbacks,
		gomock.Any(), // links
		"test-identity",
		nil, // priority
	).Return(&historypb.HistoryEvent{}, nil)
	s.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), s.shardContext).Return(nil)

	req := s.newRequest()
	req.Request.Callbacks = callbacks
	req.Request.RequestId = "test-request-id"
	req.Request.Identity = "test-identity"

	resp, err := Invoke(context.Background(), req, s.shardContext, s.workflowConsistencyChecker)
	s.NoError(err)
	s.NotNil(resp)

	notCompleted := resp.GetResponse().GetNotCompleted()
	s.NotNil(notCompleted)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, notCompleted.GetStatus())
}

func (s *getWorkflowExecutionResultSuite) TestZombieWorkflow() {
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}).AnyTimes()
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	_, err := Invoke(context.Background(), s.newRequest(), s.shardContext, s.workflowConsistencyChecker)
	s.Error(err)
}

func (s *getWorkflowExecutionResultSuite) TestContinuedAsNewWorkflow() {
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
	}).AnyTimes()
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	resp, err := Invoke(context.Background(), s.newRequest(), s.shardContext, s.workflowConsistencyChecker)
	s.NoError(err)

	// CONTINUED_AS_NEW is not terminal per API contract — should return NotCompleted.
	notCompleted := resp.GetResponse().GetNotCompleted()
	s.NotNil(notCompleted)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, notCompleted.GetStatus())
}

func (s *getWorkflowExecutionResultSuite) TestGetCompletionEventError() {
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}).AnyTimes()
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	s.currentMutableState.EXPECT().GetCompletionEvent(gomock.Any()).Return(nil, fmt.Errorf("event not found"))

	_, err := Invoke(context.Background(), s.newRequest(), s.shardContext, s.workflowConsistencyChecker)
	s.Error(err)
	s.Contains(err.Error(), "event not found")
}

func (s *getWorkflowExecutionResultSuite) TestCallbackRegistrationError() {
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  tests.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}).AnyTimes()
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.currentMutableState.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, fmt.Errorf("callback registration failed"))

	req := s.newRequest()
	req.Request.Callbacks = []*commonpb.Callback{
		{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: "http://example.com"},
			},
		},
	}

	_, err := Invoke(context.Background(), req, s.shardContext, s.workflowConsistencyChecker)
	s.Error(err)
	s.Contains(err.Error(), "callback registration failed")
}

func (s *getWorkflowExecutionResultSuite) TestInvalidNamespace() {
	req := &historyservice.GetWorkflowExecutionResultRequest{
		NamespaceId: "",
		Request: &workflowservice.GetWorkflowExecutionResultRequest{
			Namespace: tests.Namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: tests.WorkflowID,
			},
		},
	}

	_, err := Invoke(context.Background(), req, s.shardContext, s.workflowConsistencyChecker)
	s.Error(err)
}
