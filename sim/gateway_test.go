// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package sim_test

import (
	"context"
	"testing"
	"time"

	"github.com/cB-Sigma/mqttsn"
	"github.com/cB-Sigma/mqttsn-sim/sim"
	"github.com/cB-Sigma/mqttsn/gateway"
)

// The simulated gateway takes a client's publishes, survives dropped
// datagrams through the client's retransmission, and goes silent on
// demand.
// Covers: SIG-IPE-6724
func TestSimGateway(t *testing.T) {
	g, err := sim.StartGateway("127.0.0.1:0", gateway.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := sim.Dial(g.Addr(), "dev")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Connect(ctx, time.Minute, true, nil); err != nil {
		t.Fatal(err)
	}
	g.DropNext(1)
	id, err := c.Register(ctx, "dev/x")
	if err != nil {
		t.Fatalf("register after a dropped datagram: %v", err)
	}
	if err := c.Publish(ctx, mqttsn.TopicNormal, id, 1, false, []byte("1")); err != nil {
		t.Fatal(err)
	}
	if p := g.Published(); len(p) != 1 || p[0].Topic != "dev/x" {
		t.Errorf("published %+v", p)
	}
	g.Silence(true)
	sctx, scancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer scancel()
	if err := c.Ping(sctx); err == nil {
		t.Error("a silent gateway answered")
	}
}
