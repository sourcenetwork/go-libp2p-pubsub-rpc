package rpc

import (
	"context"
	"runtime"
	"testing"
	"time"

	util "github.com/ipfs/boxo/util"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResMessageHandler_NobodyReading_ReturnsWhenRequestEnds(t *testing.T) {
	tests := []struct {
		name string
		// end is called while the handler is blocked on a response nobody reads.
		end func(reqCancel, topicCancel context.CancelFunc)
	}{
		{
			name: "request ctx ends",
			end:  func(reqCancel, _ context.CancelFunc) { reqCancel() },
		},
		{
			name: "topic closes",
			end:  func(_, topicCancel context.CancelFunc) { topicCancel() },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reqCtx, reqCancel := context.WithCancel(context.Background())
			defer reqCancel()
			topic, id, msg := newTopicWithOngoing(t, reqCtx, make(chan internalResponse))
			defer topic.cancel()

			done := make(chan struct{})
			go func() {
				defer close(done)
				_, err := topic.resMessageHandler(peer.ID("responder"), "topic/_response", msg)
				assert.NoError(t, err)
			}()

			select {
			case <-done:
				t.Fatalf("handler returned before the request %s ended", id)
			case <-time.After(100 * time.Millisecond):
			}

			tc.end(reqCancel, topic.cancel)

			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("handler stayed blocked after the request ended")
			}
		})
	}
}

func TestResMessageHandler_Reading_DeliversResponse(t *testing.T) {
	respCh := make(chan internalResponse)
	topic, id, msg := newTopicWithOngoing(t, context.Background(), respCh)
	defer topic.cancel()

	go func() {
		_, err := topic.resMessageHandler(peer.ID("responder"), "topic/_response", msg)
		assert.NoError(t, err)
	}()

	select {
	case res := <-respCh:
		require.Equal(t, id.String(), res.ID)
		require.Equal(t, "pong", string(res.Data))
		require.Equal(t, []byte(peer.ID("responder")), res.From)
	case <-time.After(time.Second):
		t.Fatal("response was not delivered")
	}
}

// Many peers waiting to become reachable share one goroutine, which exits once
// they are all dropped.
func TestRepublishOnJoin_ManyUnreachablePeers_OneWaiter(t *testing.T) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	defer func() { _ = h.Close() }()
	ps, err := pubsub.NewGossipSub(context.Background(), h)
	require.NoError(t, err)
	topic, err := NewTopic(context.Background(), ps, h.ID(), "topic", true)
	require.NoError(t, err)
	defer func() { _ = topic.Close() }()

	before := runtime.NumGoroutine()
	// These peers never connect, so none of them becomes reachable.
	for i := 0; i < 100; i++ {
		topic.republishOnJoin(test.RandPeerIDFatal(t))
	}
	assert.LessOrEqual(t, runtime.NumGoroutine(), before+1)

	require.Eventually(t, func() bool {
		topic.lk.Lock()
		defer topic.lk.Unlock()
		return len(topic.waiting) == 0
	}, reachableTimeout+time.Second, 50*time.Millisecond)
}

// newTopicWithOngoing returns a topic with one ongoing request that reports
// responses on respCh, along with the request id and an encoded response to it.
func newTopicWithOngoing(
	t *testing.T,
	reqCtx context.Context,
	respCh chan internalResponse,
) (*Topic, cid.Cid, []byte) {
	id := cid.NewCidV1(cid.Raw, util.Hash([]byte("ping")))
	topic := &Topic{
		ongoing: map[cid.Cid]ongoingMessage{
			id: {ctx: reqCtx, respCh: respCh},
		},
	}
	topic.ctx, topic.cancel = context.WithCancel(context.Background())

	msg, err := ipld.Marshal(dagcbor.Encode, &internalResponse{ID: id.String(), Data: []byte("pong")}, resType)
	require.NoError(t, err)
	return topic, id, msg
}
