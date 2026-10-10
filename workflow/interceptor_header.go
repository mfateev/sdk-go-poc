package workflow

import (
	"context"
	"github.com/mfateev/sdk-go-poc/internal/headerwire"
	commonpb "go.temporal.io/api/common/v1"
)

type interceptorHeaderKey struct{}

// InterceptorHeader implements interceptor.WorkflowHeader. Headers are private
// operation-local maps. No host Payload or codec object crosses the boundary.
func InterceptorHeader(ctx context.Context) map[string]*commonpb.Payload {
	if ctx == nil {
		return nil
	}
	header, _ := ctx.Value(interceptorHeaderKey{}).(map[string]*commonpb.Payload)
	return header
}
func withInterceptorHeader(ctx context.Context, fields map[string][]byte) (context.Context, error) {
	header, err := headerwire.Decode(fields)
	if err != nil {
		return nil, err
	}
	if header == nil {
		header = &commonpb.Header{Fields: make(map[string]*commonpb.Payload)}
	}
	return context.WithValue(ctx, interceptorHeaderKey{}, header.Fields), nil
}
func outgoingHeader(ctx context.Context) (map[string][]byte, error) {
	fields := InterceptorHeader(ctx)
	if len(fields) == 0 {
		return nil, nil
	}
	return headerwire.Encode(&commonpb.Header{Fields: fields})
}
