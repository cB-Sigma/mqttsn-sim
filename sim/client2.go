package sim

import (
	"github.com/cB-Sigma/mqttsn/client"
	"github.com/cB-Sigma/mqttsn/sn2"
)

// Protection makes the client send every packet in a Protection
// Encapsulation and accept only protected packets.
type Protection = client.Protection

// Delivery2 is a message the gateway published to a 2.0 client.
type Delivery2 = client.Delivery2

// Connect2 are the CONNECT options.
type Connect2 = client.Connect2

// Subscribe2 are the SUBSCRIBE options.
type Subscribe2 = client.Subscribe2

// SearchGW2 sends a 2.0 SEARCHGW to addr and returns the first 2.0 GWINFO's
// gateway ID.
var SearchGW2 = client.SearchGW2

// Client2 is an MQTT-SN 2.0 client (OASIS draft CSD01) over UDP for tests,
// with fault switches.
type Client2 struct {
	*client.Client2
	f *faults
}

// Dial2 opens a 2.0 client socket towards the gateway at addr (host:port).
func Dial2(addr, clientID string) (*Client2, error) {
	f := &faults{}
	c, err := client.Dial2With(addr, clientID, client.Options{Filter: f.filter,
		AcceptRegister: func(uint16, string) sn2.Reason {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.refuseReg {
				return sn2.QuotaExceeded
			}
			return sn2.Success
		}})
	if err != nil {
		return nil, err
	}
	return &Client2{Client2: c, f: f}, nil
}

// Silence makes the client stop answering.
func (c *Client2) Silence() {
	c.f.mu.Lock()
	c.f.ignore = true
	c.f.mu.Unlock()
}

// RefuseRegister makes the client answer the gateway's REGISTER with a
// failure.
func (c *Client2) RefuseRegister(v bool) {
	c.f.mu.Lock()
	c.f.refuseReg = v
	c.f.mu.Unlock()
}
