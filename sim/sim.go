// Package sim has MQTT-SN clients over UDP for tests and
// demonstrations: Client speaks MQTT-SN v1.2 and Client2 MQTT-SN 2.0
// (draft CSD01). They are the clients of the mqttsn module's client
// package (since v0.2.0) with switches that make a device misbehave:
// Silence (stop answering), Congest (refuse publishes for lack of
// buffers) and RefuseRegister.
package sim

import (
	"sync"

	"github.com/cB-Sigma/mqttsn"
	"github.com/cB-Sigma/mqttsn/client"
)

// Delivery is a message the gateway published to the client.
type Delivery = client.Delivery

// Will is a last will for Connect.
type Will = client.Will

// Errors of the clients.
var (
	// ErrRejected is returned when the gateway answers with a return code.
	ErrRejected = client.ErrRejected
	// ErrDisconnected is returned by Connect when the gateway answers the
	// connection exchange with DISCONNECT instead of CONNACK.
	ErrDisconnected = client.ErrDisconnected
)

// SearchGW broadcasts (or sends) SEARCHGW to addr and returns the first
// GWINFO's gateway ID and sender.
var SearchGW = client.SearchGW

// faults are the misbehaviour switches shared by both clients.
type faults struct {
	mu        sync.Mutex
	ignore    bool
	congested int
	refuseReg bool
}

func (f *faults) filter([]byte) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.ignore
}

// Client is an MQTT-SN 1.2 client with fault switches.
type Client struct {
	*client.Client
	f *faults
}

// Dial opens a client socket towards the gateway at addr (host:port).
func Dial(addr, clientID string) (*Client, error) { return DialFrom("", addr, clientID) }

// DialFrom is Dial from the local address local (host:port), as a device
// that is given an address another device used before (NAT, DHCP).
func DialFrom(local, addr, clientID string) (*Client, error) {
	f := &faults{}
	c, err := client.DialFrom(local, addr, clientID, client.Options{Filter: f.filter,
		Accept: func(*mqttsn.Message) mqttsn.ReturnCode {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.congested > 0 {
				f.congested--
				return mqttsn.RejectedCongested
			}
			return mqttsn.Accepted
		}})
	if err != nil {
		return nil, err
	}
	return &Client{Client: c, f: f}, nil
}

// Congest makes the client answer the next n QoS 1 or 2 PUBLISHes with
// PUBACK "rejected: congestion" (a client short of buffers).
func (c *Client) Congest(n int) {
	c.f.mu.Lock()
	c.f.congested = n
	c.f.mu.Unlock()
}

// Silence makes the client stop answering (and stop sending keep-alives,
// which it never sends by itself): the gateway will declare it lost.
func (c *Client) Silence() {
	c.f.mu.Lock()
	c.f.ignore = true
	c.f.mu.Unlock()
}
