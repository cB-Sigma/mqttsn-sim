// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package sim_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/cB-Sigma/mqttsn"
	"github.com/cB-Sigma/mqttsn-sim/sim"
)

// fakeGateway answers each message with what answer returns (nil: no
// answer).
func fakeGateway(t *testing.T, answer func(*mqttsn.Message) *mqttsn.Message) string {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			m, err := mqttsn.Decode(buf[:n])
			if err != nil {
				continue
			}
			if r := answer(m); r != nil {
				_, _ = pc.WriteTo(r.Encode(), from)
			}
		}
	}()
	return pc.LocalAddr().String()
}

// A gateway that ends the will exchange with DISCONNECT (as Eclipse Paho's
// gateway does at commit e1e1b73) makes Connect fail at once with
// ErrDisconnected instead of waiting for a CONNACK that never comes.
func TestConnectDisconnectedDuringWill(t *testing.T) {
	addr := fakeGateway(t, func(m *mqttsn.Message) *mqttsn.Message {
		switch m.Type {
		case mqttsn.CONNECT:
			return &mqttsn.Message{Type: mqttsn.WILLTOPICREQ}
		case mqttsn.WILLTOPIC:
			return &mqttsn.Message{Type: mqttsn.DISCONNECT}
		}
		return nil
	})
	c, err := sim.Dial(addr, "will-client")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err = c.Connect(ctx, time.Minute, true, &sim.Will{Topic: "w", Msg: []byte("x")})
	if !errors.Is(err, sim.ErrDisconnected) {
		t.Fatalf("Connect = %v, want ErrDisconnected", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("Connect took %s", time.Since(start))
	}
}

// The will exchange and a QoS 1 publish against a gateway that follows
// the specification.
func TestConnectWillAndPublish(t *testing.T) {
	will := make(chan string, 2)
	addr := fakeGateway(t, func(m *mqttsn.Message) *mqttsn.Message {
		switch m.Type {
		case mqttsn.CONNECT:
			return &mqttsn.Message{Type: mqttsn.WILLTOPICREQ}
		case mqttsn.WILLTOPIC:
			will <- m.TopicName
			return &mqttsn.Message{Type: mqttsn.WILLMSGREQ}
		case mqttsn.WILLMSG:
			will <- string(m.Data)
			return &mqttsn.Message{Type: mqttsn.CONNACK, ReturnCode: mqttsn.Accepted}
		case mqttsn.REGISTER:
			return &mqttsn.Message{Type: mqttsn.REGACK, TopicID: 9, MsgID: m.MsgID, ReturnCode: mqttsn.Accepted}
		case mqttsn.PUBLISH:
			return &mqttsn.Message{Type: mqttsn.PUBACK, TopicID: m.TopicID, MsgID: m.MsgID, ReturnCode: mqttsn.Accepted}
		}
		return nil
	})
	c, err := sim.Dial(addr, "sensor")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx, time.Minute, true, &sim.Will{Topic: "sensors/lost", Msg: []byte("gone")}); err != nil {
		t.Fatal(err)
	}
	if willTopic, willMsg := <-will, <-will; willTopic != "sensors/lost" || willMsg != "gone" {
		t.Errorf("will %q %q", willTopic, willMsg)
	}
	id, err := c.Register(ctx, "sensors/t")
	if err != nil || id != 9 {
		t.Fatalf("register: %d %v", id, err)
	}
	if err := c.Publish(ctx, mqttsn.TopicNormal, id, 1, false, []byte("1")); err != nil {
		t.Fatal(err)
	}
}
