package adapter

import (
	"context"

	"github.com/benemon/wherry/adapter/internal/adapterv1"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
)

const pluginName = "adapter"

// handshake is protocol version 3; a plugin of another version is refused at launch. The cookie only stops a plugin
// binary started by hand from serving.
var handshake = plugin.HandshakeConfig{
	ProtocolVersion:  3,
	MagicCookieKey:   "WHERRY_ADAPTER_PLUGIN",
	MagicCookieValue: "b6f1c3e2-9d4a-4e7b-8c5f-2a1d0e9f7b63",
}

type grpcPlugin struct {
	plugin.NetRPCUnsupportedPlugin
	// server is nil on the receiver's side.
	server *server
}

func (p *grpcPlugin) GRPCServer(_ *plugin.GRPCBroker, s *grpc.Server) error {
	adapterv1.RegisterAdapterServer(s, p.server)
	return nil
}

func (p *grpcPlugin) GRPCClient(_ context.Context, _ *plugin.GRPCBroker, conn *grpc.ClientConn) (any, error) {
	return adapterv1.NewAdapterClient(conn), nil
}

func handleMessage(handle Handle) *adapterv1.Handle {
	return &adapterv1.Handle{Data: handle.Data, Name: handle.Name}
}

func handleOf(message *adapterv1.Handle) Handle {
	return Handle{Data: message.GetData(), Name: message.GetName()}
}
