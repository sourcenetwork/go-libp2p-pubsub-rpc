package rpc_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
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
			require.NotContains(t, askerPS.ListPeers("topic"), responder.ID(),
				"the gate no longer holds back sending; pubsub likely changed how it starts sending to a new peer")
			close(release)

			resp := <-respCh
			require.NoError(t, resp.Err)
			require.Equal(t, "pong", string(resp.Data))
			require.Equal(t, responder.ID(), resp.From)
		})
	}
}

// A peer that gets a request through a relay responds to the request's author.
func TestPublish_RequestDeliveredByRelay_ResponseReachesAsker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	asker, relay, responder := newHost(t), newHost(t), newHost(t)
	// The asker does not see the responder on the topic, so the request reaches it only through the relay.
	askerPS, askerTopic := newRPCTopic(ctx, t, asker,
		pubsub.WithSubscriptionFilter(hiddenSubscription{peer: responder.ID(), topic: "topic"}))
	// A direct peer gets forwarded messages without waiting to join the relay's mesh.
	relayPS, relayTopic := newRPCTopic(ctx, t, relay,
		pubsub.WithDirectPeers([]core.AddrInfo{{ID: responder.ID(), Addrs: responder.Addrs()}}))
	relayTopic.SetMessageHandler(func(core.ID, string, []byte) ([]byte, error) {
		return []byte("relay"), nil
	})
	responderPS, responderTopic := newRPCTopic(ctx, t, responder)
	responderTopic.SetMessageHandler(func(core.ID, string, []byte) ([]byte, error) {
		return []byte("responder"), nil
	})

	for _, link := range [][2]host.Host{{asker, relay}, {relay, responder}, {asker, responder}} {
		require.NoError(t, link[0].Connect(ctx, core.AddrInfo{ID: link[1].ID(), Addrs: link[1].Addrs()}))
	}
	// The responder can respond once it sees the asker on the asker's response topic.
	require.Eventually(t, func() bool {
		return hasPeer(askerPS.ListPeers("topic"), relay.ID()) &&
			hasPeer(relayPS.ListPeers("topic"), responder.ID()) &&
			hasPeer(responderPS.ListPeers("topic/"+asker.ID().String()+"/_response"), asker.ID())
	}, 5*time.Second, 10*time.Millisecond)
	require.NotContains(t, askerPS.ListPeers("topic"), responder.ID(),
		"the asker sends to the responder directly")

	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()
	respCh, err := askerTopic.Publish(reqCtx, []byte("ping"), rpc.WithMultiResponse(true))
	require.NoError(t, err)
	for resp := range respCh {
		if resp.From == responder.ID() {
			require.Equal(t, "responder", string(resp.Data))
			return
		}
	}
	t.Fatal("the responder's response never reached the asker")
}

// A message without an author is answered through the peer that delivered it.
func TestPublish_NoAuthor_ResponseReachesAsker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Messages without an author need an ID function that does not read the author.
	noAuthor := []pubsub.Option{
		pubsub.WithNoAuthor(),
		pubsub.WithMessageIdFn(func(m *pb.Message) string { return string(m.Data) }),
	}
	asker, responder := newHost(t), newHost(t)
	askerPS, askerTopic := newRPCTopic(ctx, t, asker, noAuthor...)
	responderPS, responderTopic := newRPCTopic(ctx, t, responder, noAuthor...)
	responderTopic.SetMessageHandler(func(core.ID, string, []byte) ([]byte, error) {
		return []byte("pong"), nil
	})

	require.NoError(t, asker.Connect(ctx, core.AddrInfo{ID: responder.ID(), Addrs: responder.Addrs()}))
	require.Eventually(t, func() bool {
		return hasPeer(askerPS.ListPeers("topic"), responder.ID()) &&
			hasPeer(responderPS.ListPeers("topic/"+asker.ID().String()+"/_response"), asker.ID())
	}, 5*time.Second, 10*time.Millisecond)

	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()
	respCh, err := askerTopic.Publish(reqCtx, []byte("ping"))
	require.NoError(t, err)
	resp := <-respCh
	require.NoError(t, resp.Err)
	require.Equal(t, "pong", string(resp.Data))
}

func hasPeer(peers []core.ID, p core.ID) bool {
	for _, id := range peers {
		if id == p {
			return true
		}
	}
	return false
}

func newHost(t *testing.T) host.Host {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func newRPCTopic(ctx context.Context, t *testing.T, h host.Host, opts ...pubsub.Option) (*pubsub.PubSub, *rpc.Topic) {
	// Matches how go-p2p sets up pubsub.
	ps, err := pubsub.NewGossipSub(ctx, h, append([]pubsub.Option{pubsub.WithFloodPublish(true)}, opts...)...)
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

// hiddenSubscription hides peer's subscription to topic, so this node never sends to peer on topic.
type hiddenSubscription struct {
	peer  core.ID
	topic string
}

func (hiddenSubscription) CanSubscribe(string) bool { return true }

func (f hiddenSubscription) FilterIncomingSubscriptions(
	from core.ID, subs []*pb.RPC_SubOpts,
) ([]*pb.RPC_SubOpts, error) {
	if from != f.peer {
		return subs, nil
	}
	var kept []*pb.RPC_SubOpts
	for _, s := range subs {
		if s.GetTopicid() != f.topic {
			kept = append(kept, s)
		}
	}
	return kept, nil
}
