package conncli

import "golang.org/x/net/context"

type Connd interface {
	Notify(ctx context.Context, service string, connds []string, topic string, index []byte, noneDowngrade bool) error
	Disconnect(ctx context.Context, clientID, service, conndAddress string, connetctionID int64) error
	Close() error
}
