// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Command mqttsnsim is a simulated MQTT-SN sensor: it connects to a
// gateway over UDP, registers a topic and publishes a reading every
// interval, and prints what the gateway publishes to it on the topics it
// subscribes to. It speaks MQTT-SN 1.2, or 2.0 (draft CSD01) with -v2.
//
// For example, against a gateway on the default port:
//
//	mqttsnsim -gw 127.0.0.1:1884 -id sensor-1 -topic sensors/sensor-1/temperature -sub sensors/sensor-1/cmd
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cB-Sigma/mqttsn"
	"github.com/cB-Sigma/mqttsn-sim/sim"
	"github.com/cB-Sigma/mqttsn/sn2"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "mqttsnsim:", err)
		os.Exit(1)
	}
}

type options struct {
	gw, id, topic, sub string
	every              time.Duration
	qos                int
	count              int
	v2                 bool
}

// run publishes until ctx ends, or after -count readings when it is set.
func run(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("mqttsnsim", flag.ContinueOnError)
	fs.SetOutput(out)
	var o options
	fs.StringVar(&o.gw, "gw", "127.0.0.1:1884", "gateway UDP address")
	fs.StringVar(&o.id, "id", "mqttsnsim-1", "client ID")
	fs.StringVar(&o.topic, "topic", "sensors/mqttsnsim-1/temperature", "topic the readings are published on")
	fs.StringVar(&o.sub, "sub", "", "topic filter to subscribe to (none when empty)")
	fs.DurationVar(&o.every, "every", 5*time.Second, "interval between readings")
	fs.IntVar(&o.qos, "qos", 1, "QoS of the readings (0, 1 or 2)")
	fs.IntVar(&o.count, "count", 0, "stop after this many readings (0: run until interrupted)")
	fs.BoolVar(&o.v2, "v2", false, "speak MQTT-SN 2.0 (draft CSD01) instead of 1.2")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.qos < 0 || o.qos > 2 {
		return fmt.Errorf("-qos %d is not 0, 1 or 2", o.qos)
	}
	if o.v2 {
		return run2(ctx, o, out)
	}
	return run1(ctx, o, out)
}

// reading is the n-th demo temperature, a slow saw between 20 and 25.
func reading(n int) []byte {
	return fmt.Appendf(nil, `{"temperature":%.1f}`, 20+float64(n%50)/10)
}

func run1(ctx context.Context, o options, out io.Writer) error {
	c, err := sim.Dial(o.gw, o.id)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Connect(ctx, 60*time.Second, true, nil); err != nil {
		return err
	}
	id, err := c.Register(ctx, o.topic)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "connected to %s as %s; %s is topic ID %d\n", o.gw, o.id, o.topic, id)
	if o.sub != "" {
		if _, _, err := c.Subscribe(ctx, mqttsn.TopicNormal, o.sub, 0, 1); err != nil {
			return err
		}
	}
	go func() {
		for m := range c.Messages() {
			fmt.Fprintf(out, "received %s: %s\n", m.Topic, m.Data)
		}
	}()
	return loop(ctx, o, out, func(n int) error {
		return c.Publish(ctx, mqttsn.TopicNormal, id, int8(o.qos), false, reading(n))
	}, func() { _ = c.Disconnect(context.Background()) })
}

func run2(ctx context.Context, o options, out io.Writer) error {
	c, err := sim.Dial2(o.gw, o.id)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.Connect(ctx, sim.Connect2{KeepAlive: 60 * time.Second, CleanStart: true}); err != nil {
		return err
	}
	r, err := c.Register(ctx, o.topic)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "connected to %s as %s (MQTT-SN 2.0); %s is alias %d\n", o.gw, o.id, o.topic, r.TopicAlias)
	if o.sub != "" {
		if _, err := c.Subscribe(ctx, sim.Subscribe2{TopicType: sn2.TopicName, Filter: o.sub, QoS: 1}); err != nil {
			return err
		}
	}
	go func() {
		for m := range c.Messages() {
			fmt.Fprintf(out, "received %s: %s\n", m.Topic, m.Data)
		}
	}()
	return loop(ctx, o, out, func(n int) error {
		reason, err := c.Publish(ctx, r.TopicType, r.TopicAlias, "", byte(o.qos), false, reading(n))
		if err == nil && reason.Failed() {
			err = fmt.Errorf("publish refused: reason 0x%02X", byte(reason))
		}
		return err
	}, func() { _ = c.Disconnect(sn2.Success, nil) })
}

func loop(ctx context.Context, o options, out io.Writer, publish func(int) error, bye func()) error {
	defer bye()
	t := time.NewTicker(o.every)
	defer t.Stop()
	for n := 0; ; n++ {
		if err := publish(n); err != nil {
			return err
		}
		fmt.Fprintf(out, "published %s %s\n", o.topic, reading(n))
		if o.count > 0 && n+1 >= o.count {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
