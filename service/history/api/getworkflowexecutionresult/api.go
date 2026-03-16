package getworkflowexecutionresult

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// continuedAsNewError is a sentinel error used to signal that the workflow
// has continued-as-new and we need to follow the chain to the new run.
type continuedAsNewError struct {
	newRunID string
}

func (e *continuedAsNewError) Error() string {
	return fmt.Sprintf("workflow continued as new to run %s", e.newRunID)
}

func Invoke(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionResultRequest,
	shardCtx historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.GetWorkflowExecutionResultResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if err := api.ValidateNamespaceUUID(namespaceID); err != nil {
		return nil, err
	}

	req := request.GetRequest()
	hasCallbacks := len(req.GetCallbacks()) > 0 || len(req.GetLinks()) > 0

	// Current run ID to query. Start with what the user provided (may be empty = latest).
	currentRunID := req.GetExecution().GetRunId()

	// Follow continue-as-new chain to attach callbacks to the head.
	// Max iterations to prevent infinite loops from circular chains.
	const maxChainDepth = 100
	for i := 0; i < maxChainDepth; i++ {
		resp, err := invokeOnRun(ctx, namespaceID, req, currentRunID, hasCallbacks, shardCtx, workflowConsistencyChecker)
		if err != nil {
			var canErr *continuedAsNewError
			if errors.As(err, &canErr) {
				// Follow the chain to the new run.
				currentRunID = canErr.newRunID
				continue
			}
			return nil, err
		}
		return resp, nil
	}

	return nil, serviceerror.NewInternal("exceeded maximum continue-as-new chain depth")
}

func invokeOnRun(
	ctx context.Context,
	namespaceID namespace.ID,
	req *workflowservice.GetWorkflowExecutionResultRequest,
	runID string,
	hasCallbacks bool,
	shardCtx historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.GetWorkflowExecutionResultResponse, error) {
	resp := &historyservice.GetWorkflowExecutionResultResponse{
		Response: &workflowservice.GetWorkflowExecutionResultResponse{},
	}

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			req.GetExecution().GetWorkflowId(),
			runID,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			executionState := mutableState.GetExecutionState()
			executionInfo := mutableState.GetExecutionInfo()
			namespaceName := mutableState.GetNamespaceEntry().Name().String()

			currentExecution := &commonpb.WorkflowExecution{
				WorkflowId: executionInfo.GetWorkflowId(),
				RunId:      executionState.GetRunId(),
			}

			if !mutableState.IsWorkflowExecutionRunning() {
				// Guard against zombie/corrupted states — only COMPLETED state has a completion event.
				if executionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
					return nil, consts.ErrWorkflowNotReady
				}

				// Handle CONTINUED_AS_NEW: the logical workflow chain is still running.
				if executionState.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
					if hasCallbacks {
						// Need to follow the chain to attach callbacks to the head.
						// Get the completion event to find the new run ID.
						completionEvent, err := mutableState.GetCompletionEvent(ctx)
						if err != nil {
							return nil, err
						}
						newRunID := completionEvent.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
						// Return sentinel error to signal the caller to retry with new run ID.
						return nil, &continuedAsNewError{newRunID: newRunID}
					}
					// No callbacks — just return NotCompleted.
					resp.Response.Completion = &workflowservice.GetWorkflowExecutionResultResponse_NotCompleted_{
						NotCompleted: &workflowservice.GetWorkflowExecutionResultResponse_NotCompleted{
							Execution: currentExecution,
							Status:    executionState.GetStatus(),
						},
					}
					return &api.UpdateWorkflowAction{Noop: true, CreateWorkflowTask: false}, nil
				}

				completionEvent, err := mutableState.GetCompletionEvent(ctx)
				if err != nil {
					return nil, err
				}

				result, failure, err := resultFromCompletionEvent(completionEvent)
				if err != nil {
					return nil, err
				}

				// Link to the completion event.
				resp.Response.Links = makeEventLink(namespaceName, currentExecution, completionEvent)
				resp.Response.Completion = &workflowservice.GetWorkflowExecutionResultResponse_Completed_{
					Completed: &workflowservice.GetWorkflowExecutionResultResponse_Completed{
						Execution: currentExecution,
						Status:    executionState.GetStatus(),
						Result:    result,
						Failure:   failure,
					},
				}
				return &api.UpdateWorkflowAction{Noop: true, CreateWorkflowTask: false}, nil
			}

			// Workflow is still running — build NotCompleted response.
			resp.Response.Completion = &workflowservice.GetWorkflowExecutionResultResponse_NotCompleted_{
				NotCompleted: &workflowservice.GetWorkflowExecutionResultResponse_NotCompleted{
					Execution: currentExecution,
					Status:    executionState.GetStatus(),
				},
			}

			if hasCallbacks {
				// Register callbacks via the options-updated event.
				optionsEvent, err := mutableState.AddWorkflowExecutionOptionsUpdatedEvent(
					nil,   // versioningOverride
					false, // unsetVersioningOverride
					req.GetRequestId(),
					req.GetCallbacks(),
					req.GetLinks(),
					req.GetIdentity(),
					nil, // priority
				)
				if err != nil {
					return nil, err
				}
				// Link to the options-updated event where callbacks were registered.
				resp.Response.Links = makeEventLink(namespaceName, currentExecution, optionsEvent)
				return &api.UpdateWorkflowAction{Noop: false, CreateWorkflowTask: false}, nil
			}

			// Running, no callbacks — pure read, no links.
			return &api.UpdateWorkflowAction{Noop: true, CreateWorkflowTask: false}, nil
		},
		nil,
		shardCtx,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// resultFromCompletionEvent extracts the result or failure from a workflow completion event.
// This follows the same conversion logic used when invoking workflow callbacks
// (see MutableStateImpl.GetNexusCompletion).
func resultFromCompletionEvent(event *historypb.HistoryEvent) (*commonpb.Payloads, *failurepb.Failure, error) {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		return event.GetWorkflowExecutionCompletedEventAttributes().GetResult(), nil, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		return nil, event.GetWorkflowExecutionFailedEventAttributes().GetFailure(), nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return nil, &failurepb.Failure{
			Message: "workflow execution was canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{
					Details: event.GetWorkflowExecutionCanceledEventAttributes().GetDetails(),
				},
			},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		return nil, &failurepb.Failure{
			Message: "workflow execution was terminated",
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
				TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
			},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		return nil, &failurepb.Failure{
			Message: "workflow execution timed out",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				},
			},
		}, nil
	default:
		return nil, nil, serviceerror.NewInternal(
			fmt.Sprintf("unexpected completion event type %v", event.GetEventType()),
		)
	}
}

func makeEventLink(namespaceName string, execution *commonpb.WorkflowExecution, event *historypb.HistoryEvent) []*commonpb.Link {
	return []*commonpb.Link{
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  namespaceName,
					WorkflowId: execution.GetWorkflowId(),
					RunId:      execution.GetRunId(),
					Reference: &commonpb.Link_WorkflowEvent_EventRef{
						EventRef: &commonpb.Link_WorkflowEvent_EventReference{
							EventId:   event.GetEventId(),
							EventType: event.GetEventType(),
						},
					},
				},
			},
		},
	}
}
