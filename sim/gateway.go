// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/cB-Sigma/mqttsn/gateway"
)

// Gateway is a simulated MQTT-SN 1.2 gateway for testing devices and
// clients: the mqttsn module's gateway with an in-process broker, on UDP,
// with switches that make it misbehave (Silence, DropNext).
type Gateway struct {
	*gateway.MemoryBroker
	pc     *faultyConn
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	seen   []gateway.Delivery
}

// StartGateway serves on addr ("127.0.0.1:0" for any free port) with the
// options given (predefined topics, gateway ID, retry timing).
func StartGateway(addr string, o gateway.Options) (*Gateway, error) {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{MemoryBroker: gateway.NewMemoryBroker(), pc: &faultyConn{PacketConn: pc}, cancel: cancel, done: make(chan struct{})}
	g.MemoryBroker.Subscribe("#", func(d gateway.Delivery) {
		g.mu.Lock()
		g.seen = append(g.seen, d)
		g.mu.Unlock()
	})
	go func() { _ = gateway.New(g.MemoryBroker, o).Serve(ctx, g.pc); close(g.done) }()
	return g, nil
}

// Addr is the gateway's UDP address.
func (g *Gateway) Addr() string { return g.pc.LocalAddr().String() }

// Published lists every message that reached the broker, from clients
// and from Publish.
func (g *Gateway) Published() []gateway.Delivery {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]gateway.Delivery(nil), g.seen...)
}

// Silence makes the gateway drop everything it receives (a dead gateway).
func (g *Gateway) Silence(v bool) {
	g.pc.mu.Lock()
	g.pc.silent = v
	g.pc.mu.Unlock()
}

// DropNext drops the next n datagrams the gateway receives (a lossy link:
// the clients must retransmit).
func (g *Gateway) DropNext(n int) {
	g.pc.mu.Lock()
	g.pc.drop = n
	g.pc.mu.Unlock()
}

// Close stops the gateway.
func (g *Gateway) Close() {
	g.cancel()
	select {
	case <-g.done:
	case <-time.After(5 * time.Second):
	}
}

type faultyConn struct {
	net.PacketConn
	mu     sync.Mutex
	silent bool
	drop   int
}

func (c *faultyConn) ReadFrom(b []byte) (int, net.Addr, error) {
	for {
		n, a, err := c.PacketConn.ReadFrom(b)
		if err != nil {
			return n, a, err
		}
		c.mu.Lock()
		skip := c.silent || c.drop > 0
		if c.drop > 0 {
			c.drop--
		}
		c.mu.Unlock()
		if !skip {
			return n, a, nil
		}
	}
}
