package nexusoperation

import (
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/grpc"
)

type operationContextKeyType struct{}

// OperationContextKey is the context key for OperationContext, registered as a CHASM component
// context value. Exported for use in tests that need to set up MockContext.
var OperationContextKey = operationContextKeyType{}

// OperationContext holds dependencies injected into the chasm.Context for use by Operation methods.
type OperationContext struct {
	MetricTagConfig      dynamicconfig.TypedPropertyFn[NexusMetricTagConfig]
	MaxLinksPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter
	LinkMaxSize          dynamicconfig.IntPropertyFnWithNamespaceFilter
}

// componentOnlyLibrary registers just the components without task executors or gRPC handlers.
// Used in the frontend to enable component ref serialization.
type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
	metricTagConfig      dynamicconfig.TypedPropertyFn[NexusMetricTagConfig]
	maxLinksPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter
	linkMaxSize          dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func newComponentOnlyLibrary(dc *dynamicconfig.Collection) *componentOnlyLibrary {
	return &componentOnlyLibrary{
		metricTagConfig:      MetricTagConfiguration.Get(dc),
		maxLinksPerExecution: dynamicconfig.MaxLinksPerExecution.Get(dc),
		linkMaxSize:          dynamicconfig.FrontendLinkMaxSize.Get(dc),
	}
}

func (l *componentOnlyLibrary) Name() string {
	return "nexusoperation"
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Operation](
			"operation",
			chasm.WithSearchAttributes(
				EndpointSearchAttribute,
				ServiceSearchAttribute,
				OperationSearchAttribute,
				RequestIDSearchAttribute,
				StatusSearchAttribute,
			),
			chasm.WithBusinessIDAlias("OperationId"),
			chasm.WithContextValues(map[any]any{
				OperationContextKey: &OperationContext{
					MetricTagConfig:      l.metricTagConfig,
					MaxLinksPerExecution: l.maxLinksPerExecution,
					LinkMaxSize:          l.linkMaxSize,
				},
			}),
		),
		chasm.NewRegistrableComponent[*Cancellation]("cancellation"),
	}
}

type Library struct {
	componentOnlyLibrary

	handler *handler

	operationBackoffTaskHandler                *operationBackoffTaskHandler
	operationInvocationTaskHandler             *operationInvocationTaskHandler
	operationScheduleToCloseTimeoutTaskHandler *operationScheduleToCloseTimeoutTaskHandler
	operationScheduleToStartTimeoutTaskHandler *operationScheduleToStartTimeoutTaskHandler
	operationStartToCloseTimeoutTaskHandler    *operationStartToCloseTimeoutTaskHandler

	cancellationInvocationTaskHandler *cancellationInvocationTaskHandler
	cancellationBackoffTaskHandler    *cancellationBackoffTaskHandler
}

func newLibrary(
	handler *handler,
	operationBackoffTaskHandler *operationBackoffTaskHandler,
	operationInvocationTaskHandler *operationInvocationTaskHandler,
	operationScheduleToCloseTimeoutTaskHandler *operationScheduleToCloseTimeoutTaskHandler,
	operationScheduleToStartTimeoutTaskHandler *operationScheduleToStartTimeoutTaskHandler,
	operationStartToCloseTimeoutTaskHandler *operationStartToCloseTimeoutTaskHandler,
	cancellationInvocationTaskHandler *cancellationInvocationTaskHandler,
	cancellationBackoffTaskHandler *cancellationBackoffTaskHandler,
	dc *dynamicconfig.Collection,
) *Library {
	return &Library{
		componentOnlyLibrary:                       *newComponentOnlyLibrary(dc),
		handler:                                    handler,
		operationBackoffTaskHandler:                operationBackoffTaskHandler,
		operationInvocationTaskHandler:             operationInvocationTaskHandler,
		operationScheduleToCloseTimeoutTaskHandler: operationScheduleToCloseTimeoutTaskHandler,
		operationScheduleToStartTimeoutTaskHandler: operationScheduleToStartTimeoutTaskHandler,
		operationStartToCloseTimeoutTaskHandler:    operationStartToCloseTimeoutTaskHandler,
		cancellationInvocationTaskHandler:          cancellationInvocationTaskHandler,
		cancellationBackoffTaskHandler:             cancellationBackoffTaskHandler,
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"invocation",
			l.operationInvocationTaskHandler,
			chasm.WithTaskGroup(TaskGroupName),
		),
		chasm.NewRegistrablePureTask("invocationBackoff", l.operationBackoffTaskHandler),
		chasm.NewRegistrablePureTask("scheduleToStartTimeout", l.operationScheduleToStartTimeoutTaskHandler),
		chasm.NewRegistrablePureTask("startToCloseTimeout", l.operationStartToCloseTimeoutTaskHandler),
		chasm.NewRegistrablePureTask("scheduleToCloseTimeout", l.operationScheduleToCloseTimeoutTaskHandler),
		chasm.NewRegistrableSideEffectTask(
			"cancellation",
			l.cancellationInvocationTaskHandler,
			chasm.WithTaskGroup(TaskGroupName),
		),
		chasm.NewRegistrablePureTask("cancellationBackoff", l.cancellationBackoffTaskHandler),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&nexusoperationpb.NexusOperationService_ServiceDesc, l.handler)
}
