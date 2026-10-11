// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cB-Sigma/mqttsn"
)

// gateway is a minimal MQTT-SN 1.2 gateway for the runner: it accepts the
// connection, gives the topic ID 7 and acknowledges publishes.
func gateway(t *testing.T) (string, <-chan []byte) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	got := make(chan []byte, 16)
	go func() {
		buf := make([]byte, 1500)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			m, err := mqttsn.Decode(buf[:n])
			if err != nil {
				continue
			}
			var r *mqttsn.Message
			switch m.Type {
			case mqttsn.CONNECT:
				r = &mqttsn.Message{Type: mqttsn.CONNACK, ReturnCode: mqttsn.Accepted}
			case mqttsn.REGISTER:
				r = &mqttsn.Message{Type: mqttsn.REGACK, TopicID: 7, MsgID: m.MsgID, ReturnCode: mqttsn.Accepted}
			case mqttsn.PUBLISH:
				got <- append([]byte(nil), m.Data...)
				r = &mqttsn.Message{Type: mqttsn.PUBACK, TopicID: m.TopicID, MsgID: m.MsgID, ReturnCode: mqttsn.Accepted}
			case mqttsn.DISCONNECT:
				r = &mqttsn.Message{Type: mqttsn.DISCONNECT}
			}
			if r != nil {
				_, _ = pc.WriteTo(r.Encode(), addr)
			}
		}
	}()
	return pc.LocalAddr().String(), got
}

func TestRunPublishes(t *testing.T) {
	addr, got := gateway(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, []string{"-gw", addr, "-count", "2", "-every", "10ms"}, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`{"temperature":20.0}`, `{"temperature":20.1}`} {
		select {
		case b := <-got:
			if string(b) != want {
				t.Errorf("published %s, want %s", b, want)
			}
		case <-ctx.Done():
			t.Fatal("no publish")
		}
	}
	if !strings.Contains(out.String(), "topic ID 7") {
		t.Errorf("output %q", out.String())
	}
}
