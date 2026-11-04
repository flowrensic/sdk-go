// Package activity holds the Flowrensic activity interceptor for a Temporal Go
// worker, beside go.temporal.io/sdk/activity.
package activity

import (
	"context"

	"go.temporal.io/sdk/interceptor"
)

// NewInterceptor returns the Flowrensic interceptor, to put in
// worker.Options.Interceptors. It passes every activity call through
// unchanged, inbound and outbound, and leaves workflow calls alone.
func NewInterceptor() interceptor.WorkerInterceptor {
	return &workerInterceptor{}
}

type workerInterceptor struct {
	interceptor.WorkerInterceptorBase
}

// InterceptActivity puts the inbound interceptor in front of next.
func (*workerInterceptor) InterceptActivity(
	_ context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	in := &activityInbound{}
	in.Next = next
	return in
}

type activityInbound struct {
	interceptor.ActivityInboundInterceptorBase
}

// Init puts the outbound interceptor in front of outbound.
func (a *activityInbound) Init(outbound interceptor.ActivityOutboundInterceptor) error {
	out := &activityOutbound{}
	out.Next = outbound
	return a.Next.Init(out)
}

type activityOutbound struct {
	interceptor.ActivityOutboundInterceptorBase
}
