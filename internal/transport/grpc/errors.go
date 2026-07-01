package grpc

import (
	"context"
	"errors"

	"github.com/bete7512/pulse/internal/domain"
	pkg_errors "github.com/bete7512/pulse/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toStatus maps an application/domain error to a gRPC status so clients receive a meaningful
// code instead of codes.Unknown. Errors that already carry a status (e.g. CreateSchedule's
// InvalidArgument) pass through untouched — status.FromError reports ok==true for those.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err // already a gRPC status (or nil) — don't double-wrap
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, pkg_errors.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrInvalidTransition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrInvalidSchedule):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// ServerOptions are the grpc.ServerOptions every pulse server is built with — currently the
// error-mapping interceptors. Both cmd/pulsed and the bufconn test harness use these so the
// wire behaviour is identical in production and under test.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unaryErrorInterceptor),
		grpc.ChainStreamInterceptor(streamErrorInterceptor),
	}
}

// unaryErrorInterceptor maps the error returned by any unary handler through toStatus, so the
// handlers stay thin (they just `return err`) and mapping lives in one place.
func unaryErrorInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	return resp, toStatus(err)
}

// streamErrorInterceptor is the streaming counterpart: it maps the error a stream handler
// (e.g. StreamJobs) returns when the stream ends.
func streamErrorInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return toStatus(handler(srv, ss))
}
