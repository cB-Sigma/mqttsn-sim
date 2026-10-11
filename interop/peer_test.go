// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package interop

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// image is the reference peers' image, built from ./peer.
const image = "sigma-mqttsn-interop:paho-e1e1b73"

// needDocker skips the test unless SIGMA_MQTTSN_INTEROP=1 is set and
// Docker answers, and builds the peer image when it is missing.
func needDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("SIGMA_MQTTSN_INTEROP") != "1" {
		t.Skip("set SIGMA_MQTTSN_INTEROP=1 to run the interop tests (needs Docker)")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker is not running")
	}
	if exec.Command("docker", "image", "inspect", image).Run() == nil {
		return
	}
	out, err := exec.Command("docker", "build", "-q", "-t", image, "peer").CombinedOutput()
	if err != nil {
		t.Fatalf("docker build: %v\n%s", err, out)
	}
}

// freeUDP returns a UDP port free on the loopback interface.
func freeUDP(t *testing.T) int {
	t.Helper()
	c, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

// gateway is the Paho MQTT-SN gateway with its Mosquitto broker, running
// in a container.
type gateway struct {
	t    *testing.T
	name string
	addr string // the gateway's UDP endpoint on the host
}

// startGateway runs the peer image and waits until the gateway answers.
func startGateway(t *testing.T) *gateway {
	t.Helper()
	port := freeUDP(t)
	g := &gateway{t: t, name: fmt.Sprintf("sigma-mqttsn-%d", time.Now().UnixNano()), addr: fmt.Sprintf("127.0.0.1:%d", port)}
	// -t: the gateway writes its log line by line only to a terminal.
	out, err := exec.Command("docker", "run", "-d", "-t", "--rm", "--name", g.name, "-p", fmt.Sprintf("127.0.0.1:%d:10000/udp", port), image).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		if t.Failed() || os.Getenv("SIGMA_MQTTSN_INTEROP_LOG") == "1" {
			logs, _ := exec.Command("docker", "logs", g.name).CombinedOutput()
			t.Logf("gateway output:\n%s", logs)
		}
		_ = exec.Command("docker", "rm", "-f", g.name).Run()
	})
	deadline := time.Now().Add(30 * time.Second)
	for {
		logs, _ := exec.Command("docker", "logs", g.name).CombinedOutput()
		if bytes.Contains(logs, []byte("PEER READY")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the gateway did not start:\n%s", logs)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// The gateway opens its UDP port shortly after the broker answers.
	time.Sleep(time.Second)
	return g
}

// subscription is a mosquitto_sub on the broker behind the gateway.
type subscription struct {
	cmd *exec.Cmd
	out bytes.Buffer
	err chan error
}

// subscribe starts mosquitto_sub for count messages (topic and payload per
// line, retained ones marked) within wait.
func (g *gateway) subscribe(filter string, count int, wait time.Duration) *subscription {
	g.t.Helper()
	s := &subscription{err: make(chan error, 1)}
	s.cmd = exec.Command("docker", "exec", g.name, "mosquitto_sub", "-h", "127.0.0.1", "-t", filter, "-q", "2",
		"-F", "%t %r %p", "-C", fmt.Sprint(count), "-W", fmt.Sprint(int(wait/time.Second)))
	s.cmd.Stdout, s.cmd.Stderr = &s.out, &s.out
	if err := s.cmd.Start(); err != nil {
		g.t.Fatal(err)
	}
	go func() { s.err <- s.cmd.Wait() }()
	// Give mosquitto_sub time to subscribe.
	time.Sleep(700 * time.Millisecond)
	return s
}

// lines waits for mosquitto_sub to finish and returns its lines.
func (s *subscription) lines(t *testing.T) []string {
	t.Helper()
	err := <-s.err
	text := strings.TrimSpace(s.out.String())
	if err != nil {
		t.Errorf("mosquitto_sub: %v: %s", err, text)
	}
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

// publish publishes on the broker with mosquitto_pub.
func (g *gateway) publish(topic, payload string, qos int, retain bool) {
	g.t.Helper()
	args := []string{"exec", g.name, "mosquitto_pub", "-h", "127.0.0.1", "-t", topic, "-m", payload, "-q", fmt.Sprint(qos)}
	if retain {
		args = append(args, "-r")
	}
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		g.t.Fatalf("mosquitto_pub: %v: %s", err, out)
	}
}

func testCtx(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
