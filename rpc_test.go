package rpc_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	logging "github.com/textileio/go-log/v2"
	"go.uber.org/zap/zapcore"

	rpc "github.com/sourcenetwork/go-libp2p-pubsub-rpc"
	"github.com/sourcenetwork/go-libp2p-pubsub-rpc/finalizer"
)

func init() {
	if err := setLogLevels(map[string]logging.LogLevel{
		"psrpc": logging.LevelDebug,
	}); err != nil {
		panic(err)
	}
}

func TestPingPong(t *testing.T) {
	fin := finalizer.NewFinalizer()

	p1, err := newPeer()
	require.NoError(t, err)
	fin.Add(p1)

	p2, err := newPeer()
	require.NoError(t, err)
	fin.Add(p2)

	eventHandler := func(from peer.ID, topic string, msg []byte) {
		t.Logf("%s event: %s %s", topic, from, msg)
	}
	messageHandler := func(from peer.ID, topic string, msg []byte) ([]byte, error) { // nolint:unparam
		t.Logf("%s message: %s %s", topic, from, msg)
		return []byte("pong"), nil
	}

	t1, err := rpc.NewTopic(context.Background(), p1.ps, p1.ID(), "topic", true)
	require.NoError(t, err)
	t1.SetEventHandler(eventHandler)
	t1.SetMessageHandler(messageHandler)
	fin.Add(t1)

	t2, err := p2.NewTopic(context.Background(), "topic", true)
	require.NoError(t, err)
	t2.SetEventHandler(eventHandler)
	t2.SetMessageHandler(messageHandler)
	fin.Add(t2)

	time.Sleep(time.Second * 2) // wait for mdns discovery

	// peer1 requests "pong" from peer2
	rc1, err := t1.Publish(context.Background(), []byte("ping"))
	require.NoError(t, err)
	r1 := <-rc1
	require.NotNil(t, r1)
	require.NoError(t, r1.Err)
	assert.Equal(t, "pong", string(r1.Data))
	assert.NotEmpty(t, r1.ID)
	assert.Equal(t, p2.ID().String(), r1.From.String())

	// peer2 requests "pong" from peer1
	rc2, err := t2.Publish(context.Background(), []byte("ping"))
	require.NoError(t, err)
	r2 := <-rc2
	require.NotNil(t, r2)
	require.NoError(t, r2.Err)
	assert.Equal(t, "pong", string(r2.Data))
	assert.NotEmpty(t, r2.ID)
	assert.Equal(t, p1.ID().String(), r2.From.String())

	// test ignore response - make sure nothing weird happens.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = t1.Publish(ctx, []byte("ping"), rpc.WithIgnoreResponse(true))
	require.NoError(t, err)
	cancel()

	// test republishing; peer1 requests "pong" from peer2, but peer2 joins topic after the request
	t3, err := p1.NewTopic(context.Background(), "topic2", true)
	require.NoError(t, err)
	t3.SetEventHandler(eventHandler)
	t3.SetMessageHandler(messageHandler)
	fin.Add(t3)

	lk := sync.Mutex{}
	go func() {
		time.Sleep(time.Second) // wait until after peer1 publishes the request

		t4, err := p2.NewTopic(context.Background(), "topic2", true)
		require.NoError(t, err)
		t4.SetEventHandler(eventHandler)
		t4.SetMessageHandler(messageHandler)
		lk.Lock()
		fin.Add(t4)
		lk.Unlock()
	}()

	// allow enough time for peer2 join event to be propagated.
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	rc3, err := t3.Publish(ctx, []byte("ping"), rpc.WithRepublishing(true))
	require.NoError(t, err)
	r3 := <-rc3
	require.NoError(t, r3.Err)

	lk.Lock()
	require.NoError(t, fin.Cleanup(nil))
	lk.Unlock()
}

func TestMultiPingPong(t *testing.T) {
	fin := finalizer.NewFinalizer()

	p1, err := newPeer()
	require.NoError(t, err)
	fin.Add(p1)

	p2, err := newPeer()
	require.NoError(t, err)
	fin.Add(p2)

	p3, err := newPeer()
	require.NoError(t, err)
	fin.Add(p3)

	eventHandler := func(from peer.ID, topic string, msg []byte) {
		t.Logf("%s event: %s %s", topic, from, msg)
	}
	messageHandler := func(from peer.ID, topic string, msg []byte) ([]byte, error) { // nolint:unparam
		t.Logf("%s message: %s %s", topic, from, msg)
		return []byte("pong"), nil
	}

	t1, err := p1.NewTopic(context.Background(), "topic", false)
	require.NoError(t, err)
	t1.SetEventHandler(eventHandler)
	t1.SetMessageHandler(messageHandler)
	fin.Add(t1)

	t2, err := p2.NewTopic(context.Background(), "topic", true)
	require.NoError(t, err)
	t2.SetEventHandler(eventHandler)
	t2.SetMessageHandler(messageHandler)
	fin.Add(t2)

	t3, err := p3.NewTopic(context.Background(), "topic", true)
	require.NoError(t, err)
	t3.SetEventHandler(eventHandler)
	t3.SetMessageHandler(messageHandler)
	fin.Add(t3)

	time.Sleep(time.Second * 2) // wait for mdns discovery

	// peer1 requests "pong" from peer2 and peer3
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rc, err := t1.Publish(ctx, []byte("ping"), rpc.WithMultiResponse(true))
	require.NoError(t, err)
	var pongs []struct{}
	for r := range rc {
		require.NotNil(t, r)
		require.NoError(t, r.Err)
		assert.Equal(t, "pong", string(r.Data))
		assert.NotEmpty(t, r.ID)
		pongs = append(pongs, struct{}{})
	}
	assert.Len(t, pongs, 2)

	// test republishing; peer1 requests "pong" from peer2 and peer3, but peer2 and peer3 join topic after the request
	t4, err := p1.NewTopic(context.Background(), "topic2", true)
	require.NoError(t, err)
	t4.SetEventHandler(eventHandler)
	t4.SetMessageHandler(messageHandler)
	fin.Add(t4)

	var lk sync.Mutex
	go func() {
		time.Sleep(time.Second) // wait until after peer1 publishes the request

		t5, err := p2.NewTopic(context.Background(), "topic2", true)
		require.NoError(t, err)
		t5.SetEventHandler(eventHandler)
		t5.SetMessageHandler(messageHandler)
		lk.Lock()
		fin.Add(t5)
		lk.Unlock()

		t6, err := p3.NewTopic(context.Background(), "topic2", true)
		require.NoError(t, err)
		t6.SetEventHandler(eventHandler)
		t6.SetMessageHandler(messageHandler)
		lk.Lock()
		fin.Add(t6)
		lk.Unlock()
	}()
	// allow enough time for peer2 join event to be propagated.
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel2()
	rc2, err := t4.Publish(
		ctx2,
		[]byte("ping"),
		rpc.WithMultiResponse(true),
		rpc.WithRepublishing(true),
	)
	require.NoError(t, err)
	var pongs2 []struct{}
	for r := range rc2 {
		require.NotNil(t, r)
		require.NoError(t, r.Err)
		assert.Equal(t, "pong", string(r.Data))
		assert.NotEmpty(t, r.ID)
		pongs2 = append(pongs2, struct{}{})
	}
	assert.True(t, len(pongs2) >= 2, "at least 2 responses should have been received")

	lk.Lock()
	require.NoError(t, fin.Cleanup(nil))
	lk.Unlock()
}

func setLogLevels(systems map[string]logging.LogLevel) error {
	for sys, level := range systems {
		l := zapcore.Level(level)
		if sys == "*" {
			for _, s := range logging.GetSubsystems() {
				if err := logging.SetLogLevel(s, l.CapitalString()); err != nil {
					return err
				}
			}
		}
		if err := logging.SetLogLevel(sys, l.CapitalString()); err != nil {
			return err
		}
	}
	return nil
}

// DiscoveryServiceTag is used in our mDNS advertisements to discover other chat peers.
const DiscoveryServiceTag = "pubsub-chat-example"

// discoveryNotifee gets notified when we find a new peer via mDNS discovery
type discoveryNotifee struct {
	h host.Host
}

// HandlePeerFound connects to peers discovered via mDNS. Once they're connected,
// the PubSub system will automatically start interacting with them if they also
// support PubSub.
func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	fmt.Printf("discovered new peer %s\n", pi.ID.Pretty())
	err := n.h.Connect(context.Background(), pi)
	if err != nil {
		fmt.Printf("error connecting to peer %s: %s\n", pi.ID.Pretty(), err)
	}
}

// setupDiscovery creates an mDNS discovery service and attaches it to the libp2p Host.
// This lets us automatically discover peers on the same LAN and connect to them.
func setupDiscovery(h host.Host) error {
	// setup mDNS discovery to find local peers
	s := mdns.NewMdnsService(h, DiscoveryServiceTag, &discoveryNotifee{h: h})
	return s.Start()
}

type lp2ppeer struct {
	ps *pubsub.PubSub
	host.Host
}

func newPeer() (*lp2ppeer, error) {
	ctx := context.Background()

	// create a new libp2p Host that listens on a random TCP port
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		panic(err)
	}

	// create a new PubSub service using the GossipSub router
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		panic(err)
	}

	// setup local mDNS discovery
	if err := setupDiscovery(h); err != nil {
		panic(err)
	}

	return &lp2ppeer{
		ps:   ps,
		Host: h,
	}, nil
}

func (p *lp2ppeer) NewTopic(ctx context.Context, topic string, subscribe bool) (*rpc.Topic, error) {
	return rpc.NewTopic(ctx, p.ps, p.ID(), topic, subscribe)
}
