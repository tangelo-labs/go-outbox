package causation

import (
	"context"
	"errors"
	"fmt"

	"github.com/tangelo-labs/go-grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const causationIDTransportKey = "x-causation-id"

// GRPCUnaryInterceptor is a gRPC unary call interceptor that injects an inbound causation id from gRPC metadata to the context.
func GRPCUnaryInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	cID, err := fromContextMetadata(ctx)
	if err != nil {
		return handler(ctx, req)
	}

	cCtx := AddToContext(ctx, cID)

	return handler(cCtx, req)
}

// GRPCStreamInterceptor is a gRPC stream server interceptor that injects an inbound causation id from gRPC metadata to the context.
func GRPCStreamInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()
	cID, err := fromContextMetadata(ctx)

	if err != nil {
		return handler(srv, ss)
	}

	cCtx := AddToContext(ctx, cID)
	stream := grpcx.ServerStreamWithContext(cCtx, ss)

	return handler(srv, stream)
}

// fromContextMetadata fetches the causation id from within the given context metadata.
func fromContextMetadata(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)

	if !ok {
		md, ok = metadata.FromOutgoingContext(ctx)

		if !ok {
			return "", errors.New("no metadata in context")
		}
	}

	key := md.Get(causationIDTransportKey)
	if len(key) != 1 {
		return "", fmt.Errorf("causation id key not found, include `%s` in header", causationIDTransportKey)
	}

	return key[0], nil
}
