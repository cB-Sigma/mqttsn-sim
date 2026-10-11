// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Package interop runs the MQTT-SN 1.2 client of mqttsn-sim through the
// Eclipse Paho MQTT-SN gateway (github.com/eclipse/paho.mqtt-sn.embedded-c,
// EPL-2.0 / EDL-1.0) to a Mosquitto broker, both in a container built from
// ./peer. The tests are skipped unless SIGMA_MQTTSN_INTEROP=1 is set and
// Docker is running.
package interop

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/cB-Sigma/mqttsn"
	"github.com/cB-Sigma/mqttsn-sim/sim"
)

func dial(t *testing.T, g *gateway, id string) *sim.Client {
	t.Helper()
	c, err := sim.Dial(g.addr, id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func recv(t *testing.T, c *sim.Client, within time.Duration) sim.Delivery {
	t.Helper()
	select {
	case d := <-c.Messages():
		return d
	case <-time.After(within):
		t.Fatalf("no publish from the gateway within %s", within)
		return sim.Delivery{}
	}
}

// TestPublishThroughPaho: the client connects to Paho's gateway, registers
// a topic and publishes at QoS 0, 1 and 2, with a short topic name, a
// predefined topic ID and the retain flag; each message reaches the broker
// on the right topic.
func TestPublishThroughPaho(t *testing.T) {
	needDocker(t)
	g := startGateway(t)
	ctx := testCtx(t, 60*time.Second)
	c := dial(t, g, "sigma-pub")
	if err := c.Connect(ctx, 60*time.Second, true, nil); err != nil {
		t.Fatal(err)
	}

	sub := g.subscribe("#", 6, 20*time.Second)
	id, err := c.Register(ctx, "sigma/sensor/temperature")
	if err != nil {
		t.Fatal(err)
	}
	for qos, payload := range []string{"20.5", "20.6", "20.7"} {
		if err := c.Publish(ctx, mqttsn.TopicNormal, id, int8(qos), false, []byte(payload)); err != nil {
			t.Fatalf("QoS %d: %v", qos, err)
		}
	}
	short, _ := mqttsn.ShortTopic("ab")
	if err := c.Publish(ctx, mqttsn.TopicShort, short, 1, false, []byte("short")); err != nil {
		t.Fatalf("short topic: %v", err)
	}
	if err := c.Publish(ctx, mqttsn.TopicPredefined, 1, 1, false, []byte("predefined")); err != nil {
		t.Fatalf("predefined topic: %v", err)
	}
	if err := c.Publish(ctx, mqttsn.TopicNormal, id, 1, true, []byte("retained")); err != nil {
		t.Fatalf("retained: %v", err)
	}
	got := sub.lines(t)
	want := []string{
		"sigma/sensor/temperature 0 20.5",
		"sigma/sensor/temperature 0 20.6",
		"sigma/sensor/temperature 0 20.7",
		"ab 0 short",
		"sigma/predefined/one 0 predefined",
		"sigma/sensor/temperature 0 retained",
	}
	if !slices.Equal(got, want) {
		t.Errorf("broker received\n%q\nwant\n%q", got, want)
	}
	// A new subscriber gets the retained message.
	if got := g.subscribe("sigma/sensor/temperature", 1, 10*time.Second).lines(t); !slices.Equal(got, []string{"sigma/sensor/temperature 1 retained"}) {
		t.Errorf("retained message: %q", got)
	}
	if err := c.Ping(ctx); err != nil {
		t.Errorf("ping: %v", err)
	}
	if err := c.Disconnect(ctx); err != nil {
		t.Errorf("disconnect: %v", err)
	}
}

// TestSubscribeThroughPaho: the client subscribes with a wildcard, a short
// topic name and a predefined topic ID at QoS 1 and 2; the gateway
// registers the topics the broker sends and delivers each message; after
// unsubscribing nothing more arrives.
func TestSubscribeThroughPaho(t *testing.T) {
	needDocker(t)
	g := startGateway(t)
	ctx := testCtx(t, 60*time.Second)
	c := dial(t, g, "sigma-sub")
	if err := c.Connect(ctx, 60*time.Second, true, nil); err != nil {
		t.Fatal(err)
	}
	if _, q, err := c.Subscribe(ctx, mqttsn.TopicNormal, "sigma/down/#", 0, 1); err != nil || q != 1 {
		t.Fatalf("subscribe wildcard: granted %d, %v", q, err)
	}
	if _, q, err := c.Subscribe(ctx, mqttsn.TopicShort, "zz", 0, 2); err != nil || q != 2 {
		t.Fatalf("subscribe short: granted %d, %v", q, err)
	}
	if _, q, err := c.Subscribe(ctx, mqttsn.TopicPredefined, "", 2, 1); err != nil || q != 1 {
		t.Fatalf("subscribe predefined: granted %d, %v", q, err)
	}

	g.publish("sigma/down/lamp", "on", 1, false)
	d := recv(t, c, 10*time.Second)
	if d.Topic != "sigma/down/lamp" || string(d.Data) != "on" || d.QoS != 1 || d.IDType != mqttsn.TopicNormal {
		t.Errorf("wildcard delivery %+v", d)
	}
	g.publish("sigma/down/lamp", "off", 2, false)
	if d := recv(t, c, 10*time.Second); d.Topic != "sigma/down/lamp" || string(d.Data) != "off" || d.QoS != 1 {
		t.Errorf("QoS 2 publish on a QoS 1 subscription: %+v", d)
	}
	g.publish("zz", "short", 2, false)
	if d := recv(t, c, 10*time.Second); d.Topic != "zz" || string(d.Data) != "short" || d.QoS != 2 || d.IDType != mqttsn.TopicShort {
		t.Errorf("short delivery %+v", d)
	}
	g.publish("sigma/predefined/two", "pre", 1, false)
	if d := recv(t, c, 10*time.Second); d.TopicID != 2 || string(d.Data) != "pre" || d.IDType != mqttsn.TopicPredefined {
		t.Errorf("predefined delivery %+v", d)
	}

	if err := c.Unsubscribe(ctx, "sigma/down/#"); err != nil {
		t.Fatal(err)
	}
	g.publish("sigma/down/lamp", "gone", 1, false)
	select {
	case d := <-c.Messages():
		t.Errorf("delivered after unsubscribe: %+v", d)
	case <-time.After(2 * time.Second):
	}
}

// TestWillThroughPaho: a client that connects with a will and then goes
// silent is declared lost by the gateway, which publishes the will.
func TestWillThroughPaho(t *testing.T) {
	needDocker(t)
	g := startGateway(t)
	ctx := testCtx(t, 90*time.Second)
	c := dial(t, g, "sigma-will")
	sub := g.subscribe("sigma/will/#", 1, 60*time.Second)
	err := c.Connect(ctx, 5*time.Second, true, &sim.Will{Topic: "sigma/will/sigma-will", Msg: []byte("lost"), QoS: 1})
	if errors.Is(err, sim.ErrDisconnected) {
		// Paho e1e1b73 asks for the will topic, then discards the
		// WILLTOPIC because the client is still in its Disconnected state
		// until the broker CONNECT is sent (MQTTSNGWConnectionHandler.cpp
		// handleConnect, MQTTSNGWClientRecvTask.cpp), and answers with
		// DISCONNECT. A will cannot be set through this gateway.
		t.Skipf("Paho refused the will exchange: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	c.Silence()
	if got := sub.lines(t); !slices.Equal(got, []string{"sigma/will/sigma-will 0 lost"}) {
		t.Errorf("will: %q", got)
	}
}

// TestSleepThroughPaho: a client subscribes, goes to sleep, and gets the
// message the broker sent meanwhile when it wakes up.
func TestSleepThroughPaho(t *testing.T) {
	needDocker(t)
	g := startGateway(t)
	ctx := testCtx(t, 60*time.Second)
	c := dial(t, g, "sigma-sleep")
	if err := c.Connect(ctx, 60*time.Second, true, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Subscribe(ctx, mqttsn.TopicNormal, "sigma/sleep/cmd", 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := c.Sleep(ctx, 60*time.Second); err != nil {
		t.Fatal(err)
	}
	g.publish("sigma/sleep/cmd", "wake-up", 1, false)
	time.Sleep(time.Second)
	select {
	case d := <-c.Messages():
		t.Fatalf("delivered while asleep: %+v", d)
	default:
	}
	// Paho e1e1b73 delivers the stored messages on the PINGREQ with the
	// client ID, but sends the PINGRESP that ends the awake period (MQTT-SN
	// 1.2 clause 6.14) only when it had to REGISTER a topic first
	// (MQTTSNGWConnectionHandler.cpp holds the PINGREQ; only handleRegAck
	// releases it). Here the topic ID is known from the SUBACK, so Wake
	// sees no PINGRESP.
	wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	wakeErr := c.Wake(wctx)
	if d := recv(t, c, 5*time.Second); d.Topic != "sigma/sleep/cmd" || string(d.Data) != "wake-up" {
		t.Errorf("queued delivery %+v", d)
	}
	if wakeErr != nil {
		t.Logf("no PINGRESP after the stored messages (Paho holds it unless a REGISTER was needed): %v", wakeErr)
	}
	// A further PINGREQ is answered.
	if err := c.Ping(ctx); err != nil {
		t.Errorf("ping after waking: %v", err)
	}
}

// TestSleepRegisterThroughPaho: a client that subscribed with a wildcard
// sleeps; the gateway stores a message on a topic it has not registered
// with the client, and on waking registers it, delivers the message and
// sends PINGRESP.
func TestSleepRegisterThroughPaho(t *testing.T) {
	needDocker(t)
	g := startGateway(t)
	ctx := testCtx(t, 60*time.Second)
	c := dial(t, g, "sigma-sleep2")
	if err := c.Connect(ctx, 60*time.Second, true, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Subscribe(ctx, mqttsn.TopicNormal, "sigma/sleep/#", 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := c.Sleep(ctx, 60*time.Second); err != nil {
		t.Fatal(err)
	}
	g.publish("sigma/sleep/valve", "close", 1, false)
	time.Sleep(time.Second)
	if err := c.Wake(ctx); err != nil {
		t.Fatalf("wake: %v", err)
	}
	if d := recv(t, c, 5*time.Second); d.Topic != "sigma/sleep/valve" || string(d.Data) != "close" {
		t.Errorf("queued delivery %+v", d)
	}
}
