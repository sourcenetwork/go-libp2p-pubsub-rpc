package rpc_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	core "github.com/libp2p/go-libp2p/core/peer"
	rpc "github.com/sourcenetwork/go-libp2p-pubsub-rpc"
	"github.com/stretchr/testify/require"
)

// A request must reach a peer that pubsub reports as joined before this node
// can send to it, whether the peer joined before or after the request was
// published.
func TestPublish_PeerJoinedButNotYetReachable_GetsRequest(t *testing.T) {
	tests := []struct {
		name          string
		publishBefore bool
	}{
		{name: "peer joined before the request", publishBefore: false},
		{name: "peer joined after the request", publishBefore: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// The asker cannot send to the responder until release is closed,
			// while the responder's join still reaches it.
			release := make(chan struct{})
			asker := &gatedHost{Host: newHost(t), release: release}
			responder := newHost(t)

			askerPS, askerTopic := newRPCTopic(ctx, t, asker)
			joined := make(chan struct{})
			var once sync.Once
			askerTopic.SetEventHandler(func(from core.ID, _ string, msg []byte) {
				if from == responder.ID() && string(msg) == "JOINED" {
					once.Do(func() { close(joined) })
				}
			})

			_, responderTopic := newRPCTopic(ctx, t, responder)
			responderTopic.SetMessageHandler(func(core.ID, string, []byte) ([]byte, error) {
				return []byte("pong"), nil
			})

			reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
			defer reqCancel()

			var respCh <-chan rpc.Response
			publish := func() {
				var err error
				respCh, err = askerTopic.Publish(reqCtx, []byte("ping"), rpc.WithRepublishing(true))
				require.NoError(t, err)
			}

			if tc.publishBefore {
				publish()
			}
			require.NoError(t, asker.Connect(ctx, core.AddrInfo{ID: responder.ID(), Addrs: responder.Addrs()}))
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Fatal("asker never saw the responder join")
			}
			if !tc.publishBefore {
				publish()
			}

			// Let any send attempt made while unreachable happen, then make
			// the responder reachable.
			time.Sleep(200 * time.Millisecond)
			require.False(t, slices.Contains(askerPS.ListPeers("topic"), responder.ID()),
				"the gate no longer holds back sending; pubsub likely changed how it starts sending to a new peer")
			close(release)

			resp := <-respCh
			require.NoError(t, resp.Err)
			require.Equal(t, "pong", string(resp.Data))
			require.Equal(t, responder.ID(), resp.From)
		})
	}
}

func newHost(t *testing.T) host.Host {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func newRPCTopic(ctx context.Context, t *testing.T, h host.Host) (*pubsub.PubSub, *rpc.Topic) {
	// Matches how go-p2p sets up pubsub.
	ps, err := pubsub.NewGossipSub(ctx, h, pubsub.WithFloodPublish(true))
	require.NoError(t, err)
	topic, err := rpc.NewTopic(ctx, ps, h.ID(), "topic", true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = topic.Close() })
	return ps, topic
}

// gatedHost delays the connect notification pubsub waits for before it can send
// to a new peer, until release is closed.
type gatedHost struct {
	host.Host
	release chan struct{}
}

func (h *gatedHost) Network() network.Network {
	return &gatedNetwork{Network: h.Host.Network(), release: h.release}
}

type gatedNetwork struct {
	network.Network
	release chan struct{}
}

func (n *gatedNetwork) Notify(nf network.Notifiee) {
	n.Network.Notify(&gatedNotifiee{Notifiee: nf, release: n.release})
}

type gatedNotifiee struct {
	network.Notifiee
	release chan struct{}
}

func (nf *gatedNotifiee) Connected(n network.Network, c network.Conn) {
	go func() {
		<-nf.release
		nf.Notifiee.Connected(n, c)
	}()
}
