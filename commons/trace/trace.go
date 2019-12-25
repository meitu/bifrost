package trace

import (
	"context"
	"errors"

	"google.golang.org/grpc/metadata"
)

var ErrTraceID = errors.New("tardis : traceid is empty")

func WithTraceID(ctx context.Context, traceid string) context.Context {
	send := metadata.Pairs("tardis-trace-id", traceid)
	origin, _ := metadata.FromOutgoingContext(ctx)
	return metadata.NewOutgoingContext(ctx, metadata.Join(origin, send))
}

func GetTraceID(ctx context.Context) (traceid string, err error) {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if len(md["tardis-trace-id"]) > 0 {
			traceid = md["tardis-trace-id"][0]
			return
		}
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if len(md["tardis-trace-id"]) > 0 {
			traceid = md["tardis-trace-id"][0]
			return
		}
	}
	return traceid, ErrTraceID
}
