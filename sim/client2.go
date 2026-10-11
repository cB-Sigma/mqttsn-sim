package sim

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cB-Sigma/iotcrypto/sasl"
	"github.com/cB-Sigma/mqttsn/sn2"
)

// Client2 is an MQTT-SN 2.0 client (OASIS draft CSD01) over UDP for tests:
// it connects (with a will, session expiry and MQTT-BASIC credentials),
// registers topic aliases, publishes at every QoS and with PUBWOS,
// subscribes, sleeps and wakes, and disconnects with a reason. Requests
// are sequential: one call at a time per Client2. It acknowledges the
// gateway's REGISTER and PUBLISH by itself.
type Client2 struct {
	ID string

	conn *net.UDPConn
	gw   *net.UDPAddr
	resp chan *sn2.Packet
	msgs chan Delivery2

	mu      sync.Mutex
	aliases map[uint16]string // session topic aliases
	nextID  uint16
	ignore  bool
	// RefuseRegister makes the client refuse the gateway's REGISTER.
	refuseRegister bool
	done           chan struct{}

	prot    *Protection
	counter uint32
	// Rejected counts datagrams the client could not unprotect.
	Rejected atomic.Int32
}

// Protection makes the client send every packet in a Protection
// Encapsulation and accept only protected packets.
type Protection struct {
	Scheme   byte
	SenderID [8]byte
	Key      []byte
	// Counter: 2 or 4 to send a Monotonic Counter, 0 for none.
	Counter int
	// TagCode: 0 or 1 for the nominal tag, 4 to 15 to truncate.
	TagCode byte
}

// Protect turns protection on (nil: off).
func (c *Client2) Protect(p *Protection) {
	c.mu.Lock()
	c.prot = p
	c.mu.Unlock()
}

// Wrap protects b as the client would (for replay tests).
func (c *Client2) Wrap(b []byte) []byte {
	c.mu.Lock()
	p := c.prot
	c.counter++
	ctr := c.counter
	c.mu.Unlock()
	if p == nil {
		return b
	}
	h := sn2.Protection{Scheme: p.Scheme, SenderID: p.SenderID, CounterLen: p.Counter, Counter: ctr, TagCode: p.TagCode}
	_, _ = rand.Read(h.Random[:])
	out, err := sn2.Protect(p.Key, h, b)
	if err != nil {
		return nil
	}
	return out
}

// Delivery2 is a message the gateway published to a 2.0 client.
type Delivery2 struct {
	Topic      string // resolved name ("" for an unknown alias)
	TopicType  byte
	TopicAlias uint16
	QoS        byte
	DUP        bool
	Retain     bool
	Data       []byte
}

// Dial2 opens a 2.0 client socket towards the gateway at addr (host:port).
func Dial2(addr, clientID string) (*Client2, error) {
	gw, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: gw.IP.To4(), Port: 0})
	if err != nil {
		return nil, err
	}
	c := &Client2{
		ID: clientID, conn: conn, gw: gw,
		resp: make(chan *sn2.Packet, 64), msgs: make(chan Delivery2, 256),
		aliases: map[uint16]string{}, done: make(chan struct{}),
	}
	go c.read()
	return c, nil
}

// Close closes the socket without telling the gateway.
func (c *Client2) Close() {
	_ = c.conn.Close()
	<-c.done
}

// LocalAddr is the client's address.
func (c *Client2) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// Messages are the gateway's publishes to this client.
func (c *Client2) Messages() <-chan Delivery2 { return c.msgs }

// Silence makes the client stop answering.
func (c *Client2) Silence() {
	c.mu.Lock()
	c.ignore = true
	c.mu.Unlock()
}

// RefuseRegister makes the client answer the gateway's REGISTER with a
// failure.
func (c *Client2) RefuseRegister(v bool) {
	c.mu.Lock()
	c.refuseRegister = v
	c.mu.Unlock()
}

// Send writes a packet to the gateway.
func (c *Client2) Send(p *sn2.Packet) error { return c.SendRaw(p.Encode()) }

// SendRaw writes bytes to the gateway.
func (c *Client2) SendRaw(b []byte) error {
	_, err := c.conn.WriteToUDP(c.Wrap(b), c.gw)
	return err
}

// SendUnprotected writes bytes as they are.
func (c *Client2) SendUnprotected(b []byte) error {
	_, err := c.conn.WriteToUDP(b, c.gw)
	return err
}

// Alias is the topic name the gateway registered for a session alias.
func (c *Client2) Alias(id uint16) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.aliases[id]
}

func (c *Client2) read() {
	defer close(c.done)
	buf := make([]byte, 65536)
	for {
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		b := buf[:n]
		c.mu.Lock()
		prot := c.prot
		c.mu.Unlock()
		if prot != nil {
			_, inner, err := sn2.Unprotect(b, func([8]byte) ([]byte, bool) { return prot.Key, true })
			if err != nil {
				c.Rejected.Add(1)
				continue
			}
			b = inner
		}
		p, err := sn2.Decode(b)
		if err != nil {
			continue
		}
		c.mu.Lock()
		ignore, refuse := c.ignore, c.refuseRegister
		c.mu.Unlock()
		if ignore {
			continue
		}
		switch p.Type {
		case sn2.REGISTER:
			ack := &sn2.Packet{Type: sn2.REGACK, PacketID: p.PacketID, HasReason: true}
			if refuse {
				ack.Reason = sn2.QuotaExceeded
			} else {
				c.mu.Lock()
				c.aliases[p.TopicAlias] = p.TopicName
				c.mu.Unlock()
			}
			_ = c.Send(ack)
		case sn2.PUBLISH:
			c.onPublish(p)
		case sn2.PUBREL:
			_ = c.Send(&sn2.Packet{Type: sn2.PUBCOMP, PacketID: p.PacketID})
		default:
			select {
			case c.resp <- p:
			default:
			}
		}
	}
}

func (c *Client2) onPublish(p *sn2.Packet) {
	d := Delivery2{TopicType: p.TopicType, TopicAlias: p.TopicAlias, QoS: p.QoS, DUP: p.DUP, Retain: p.Retain, Data: p.Payload}
	switch p.TopicType {
	case sn2.TopicName:
		d.Topic = p.TopicName
	case sn2.TopicSession:
		d.Topic = c.Alias(p.TopicAlias)
	}
	switch p.QoS {
	case 1:
		_ = c.Send(&sn2.Packet{Type: sn2.PUBACK, PacketID: p.PacketID})
	case 2:
		_ = c.Send(&sn2.Packet{Type: sn2.PUBREC, PacketID: p.PacketID})
	}
	select {
	case c.msgs <- d:
	default:
	}
}

// Next waits for the gateway's next packet of type t (other packets
// that are not publishes are discarded).
func (c *Client2) Next(ctx context.Context, t sn2.Type) (*sn2.Packet, error) {
	return c.await(ctx, func(p *sn2.Packet) bool { return p.Type == t })
}

func (c *Client2) await(ctx context.Context, ok func(*sn2.Packet) bool) (*sn2.Packet, error) {
	for {
		select {
		case p := <-c.resp:
			if ok(p) {
				return p, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// request sends p, retransmitting it, until a packet of type want with
// p's packet identifier arrives.
func (c *Client2) request(ctx context.Context, p *sn2.Packet, want ...sn2.Type) (*sn2.Packet, error) {
	for attempt := 0; ; attempt++ {
		if err := c.Send(p); err != nil {
			return nil, err
		}
		actx, cancel := context.WithTimeout(ctx, 2*time.Second)
		r, err := c.await(actx, func(r *sn2.Packet) bool {
			for _, w := range want {
				if r.Type == w && r.PacketID == p.PacketID {
					return true
				}
			}
			return false
		})
		cancel()
		if err == nil {
			return r, nil
		}
		if ctx.Err() != nil || attempt >= 3 {
			return nil, fmt.Errorf("no %v from the gateway: %w", want, err)
		}
		if p.Type == sn2.PUBLISH && p.QoS == 2 {
			p.DUP = true
		}
	}
}

// PacketID returns a new packet identifier.
func (c *Client2) PacketID() uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	if c.nextID == 0 {
		c.nextID = 1
	}
	return c.nextID
}

// Connect2 are the CONNECT options.
type Connect2 struct {
	KeepAlive     time.Duration
	CleanStart    bool
	SessionExpiry *uint32
	Will          *sn2.Will
	Auth          *sn2.Auth
	DefaultAwake  *byte
	AllowAddr     bool
	// AllowServerValues lets the gateway change Keep Alive, Session
	// Expiry and Sleep Duration (clause 3.1.2.7).
	AllowServerValues bool
	MaxPacketSize     uint16
}

// Connect sends CONNECT and returns the CONNACK (not retried: clause 4.4.2-1).
func (c *Client2) Connect(ctx context.Context, o Connect2) (*sn2.Packet, error) {
	p := c.connectPacket(o)
	if err := c.Send(p); err != nil {
		return nil, err
	}
	actx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	r, err := c.await(actx, func(r *sn2.Packet) bool { return r.Type == sn2.CONNACK && r.PacketID == p.PacketID })
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if r.Reason.Failed() {
		return r, fmt.Errorf("%w: CONNACK reason 0x%02X", ErrRejected, byte(r.Reason))
	}
	if r.ClientID != "" {
		c.ID = r.ClientID
	}
	return r, nil
}

// ConnectSCRAM connects with a SCRAM method (clause 4.11.1): the client's
// first message in CONNECT, the final one in AUTH; the CONNACK carries the
// server's verifier, which is checked.
func (c *Client2) ConnectSCRAM(ctx context.Context, o Connect2, mech, user, password string) (*sn2.Packet, error) {
	var ack *sn2.Packet
	err := sasl.Client(mech, user, password, func(msg []byte, last bool) ([]byte, error) {
		actx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if !last {
			o.Auth = &sn2.Auth{Method: mech, Data: msg}
			p := c.connectPacket(o)
			if err := c.Send(p); err != nil {
				return nil, err
			}
			r, err := c.await(actx, func(r *sn2.Packet) bool {
				return (r.Type == sn2.AUTH || r.Type == sn2.CONNACK) && r.PacketID == p.PacketID
			})
			if err != nil {
				return nil, err
			}
			if r.Type == sn2.CONNACK || r.Reason != sn2.ContinueAuthentication || r.Auth == nil {
				ack = r
				return nil, fmt.Errorf("%w: reason 0x%02X", ErrRejected, byte(r.Reason))
			}
			return r.Auth.Data, nil
		}
		a := &sn2.Packet{Type: sn2.AUTH, PacketID: c.PacketID(), Reason: sn2.ContinueAuthentication, Auth: &sn2.Auth{Method: mech, Data: msg}}
		if err := c.Send(a); err != nil {
			return nil, err
		}
		r, err := c.await(actx, func(r *sn2.Packet) bool { return r.Type == sn2.CONNACK && r.PacketID == a.PacketID })
		if err != nil {
			return nil, err
		}
		ack = r
		if r.Reason.Failed() || r.Auth == nil {
			return nil, fmt.Errorf("%w: CONNACK reason 0x%02X", ErrRejected, byte(r.Reason))
		}
		return r.Auth.Data, nil
	})
	return ack, err
}

// ReauthSCRAM re-authenticates with AUTH 0x19 (clause 4.11.1.1).
func (c *Client2) ReauthSCRAM(ctx context.Context, mech, user, password string) error {
	return sasl.Client(mech, user, password, func(msg []byte, last bool) ([]byte, error) {
		reason := sn2.ReAuthenticate
		if last {
			reason = sn2.ContinueAuthentication
		}
		a := &sn2.Packet{Type: sn2.AUTH, PacketID: c.PacketID(), Reason: reason, Auth: &sn2.Auth{Method: mech, Data: msg}}
		if err := c.Send(a); err != nil {
			return nil, err
		}
		actx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		r, err := c.await(actx, func(r *sn2.Packet) bool {
			return (r.Type == sn2.AUTH && r.PacketID == a.PacketID) || r.Type == sn2.DISCONNECT
		})
		if err != nil {
			return nil, err
		}
		if r.Type == sn2.DISCONNECT || r.Auth == nil {
			return nil, fmt.Errorf("%w: %s reason 0x%02X", ErrRejected, r.Type, byte(r.Reason))
		}
		return r.Auth.Data, nil
	})
}

func (c *Client2) connectPacket(o Connect2) *sn2.Packet {
	p := &sn2.Packet{Type: sn2.CONNECT, PacketID: c.PacketID(), Version: sn2.ProtocolVersion,
		KeepAlive: uint16(o.KeepAlive / time.Second), CleanStart: o.CleanStart, Will: o.Will, Auth: o.Auth,
		AllowAddrChange: o.AllowAddr, AllowServerValues: o.AllowServerValues, MaxPacketSize: o.MaxPacketSize, ClientID: c.ID}
	if o.SessionExpiry != nil {
		p.HasSessionExpiry, p.SessionExpiry = true, *o.SessionExpiry
	}
	if o.DefaultAwake != nil {
		p.HasDefaultAwake, p.DefaultAwake = true, *o.DefaultAwake
	}
	return p
}

// Register asks for a session topic alias; the REGACK is returned.
func (c *Client2) Register(ctx context.Context, topic string) (*sn2.Packet, error) {
	r, err := c.request(ctx, &sn2.Packet{Type: sn2.REGISTER, PacketID: c.PacketID(), TopicName: topic}, sn2.REGACK)
	if err != nil {
		return nil, err
	}
	if r.Reason.Failed() {
		return r, fmt.Errorf("%w: REGACK reason 0x%02X", ErrRejected, byte(r.Reason))
	}
	if r.TopicType == sn2.TopicSession {
		c.mu.Lock()
		c.aliases[r.TopicAlias] = topic
		c.mu.Unlock()
	}
	return r, nil
}

// Publish publishes at QoS 0, 1 or 2 by topic type (session alias,
// predefined alias or topic name), completing the exchange; it returns
// the reason code of PUBACK or PUBREC (Success for QoS 0).
func (c *Client2) Publish(ctx context.Context, topicType byte, alias uint16, name string, qos byte, retain bool, data []byte) (sn2.Reason, error) {
	p := &sn2.Packet{Type: sn2.PUBLISH, QoS: qos, Retain: retain, TopicType: topicType, TopicAlias: alias, TopicName: name, Payload: data}
	if qos == 0 {
		return sn2.Success, c.Send(p)
	}
	p.PacketID = c.PacketID()
	want := sn2.PUBACK
	if qos == 2 {
		want = sn2.PUBREC
	}
	r, err := c.request(ctx, p, want)
	if err != nil {
		return 0, err
	}
	if r.Reason.Failed() || qos == 1 {
		return r.Reason, nil
	}
	r2, err := c.request(ctx, &sn2.Packet{Type: sn2.PUBREL, PacketID: p.PacketID}, sn2.PUBCOMP)
	if err != nil {
		return 0, err
	}
	return r2.Reason, nil
}

// PubWOS publishes without a session (by predefined alias or topic name).
func (c *Client2) PubWOS(topicType byte, alias uint16, name string, retain bool, data []byte) error {
	return c.Send(&sn2.Packet{Type: sn2.PUBWOS, TopicType: topicType, TopicAlias: alias, TopicName: name, Retain: retain, Payload: data})
}

// Subscribe2 are the SUBSCRIBE options.
type Subscribe2 struct {
	TopicType         byte
	Filter            string
	Alias             uint16
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
}

// Subscribe sends SUBSCRIBE and returns the SUBACK; a session alias in it
// is remembered.
func (c *Client2) Subscribe(ctx context.Context, o Subscribe2) (*sn2.Packet, error) {
	p := &sn2.Packet{Type: sn2.SUBSCRIBE, PacketID: c.PacketID(), TopicType: o.TopicType, TopicName: o.Filter, TopicAlias: o.Alias,
		QoS: o.QoS, NoLocal: o.NoLocal, RetainAsPublished: o.RetainAsPublished, RetainHandling: o.RetainHandling}
	r, err := c.request(ctx, p, sn2.SUBACK)
	if err != nil {
		return nil, err
	}
	if r.Reason.Failed() {
		return r, fmt.Errorf("%w: SUBACK reason 0x%02X", ErrRejected, byte(r.Reason))
	}
	if r.HasTopicAlias && r.TopicType == sn2.TopicSession {
		c.mu.Lock()
		c.aliases[r.TopicAlias] = o.Filter
		c.mu.Unlock()
	}
	return r, nil
}

// Unsubscribe removes a subscription by topic filter; the UNSUBACK is returned.
func (c *Client2) Unsubscribe(ctx context.Context, filter string) (*sn2.Packet, error) {
	return c.request(ctx, &sn2.Packet{Type: sn2.UNSUBSCRIBE, PacketID: c.PacketID(), TopicType: sn2.TopicName, TopicName: filter}, sn2.UNSUBACK)
}

// Ping sends PINGREQ and returns PINGRESP.
func (c *Client2) Ping(ctx context.Context) (*sn2.Packet, error) {
	return c.request(ctx, &sn2.Packet{Type: sn2.PINGREQ, PacketID: c.PacketID()}, sn2.PINGRESP)
}

// Sleep sends SLEEPREQ and returns SLEEPRESP.
func (c *Client2) Sleep(ctx context.Context, d time.Duration, retainAliases bool) (*sn2.Packet, error) {
	return c.request(ctx, &sn2.Packet{Type: sn2.SLEEPREQ, PacketID: c.PacketID(), SleepDuration: uint32(d / time.Second), RetainAliases: retainAliases}, sn2.SLEEPRESP)
}

// Wake sends PINGREQ from the Asleep state and waits for the PINGRESP that
// follows the queued messages (read them from Messages).
func (c *Client2) Wake(ctx context.Context) (*sn2.Packet, error) {
	p := &sn2.Packet{Type: sn2.PINGREQ, PacketID: c.PacketID()}
	if err := c.Send(p); err != nil {
		return nil, err
	}
	actx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.await(actx, func(r *sn2.Packet) bool { return r.Type == sn2.PINGRESP && r.PacketID == p.PacketID })
}

// Disconnect sends DISCONNECT with a reason code and, if not nil, a new
// session expiry interval. There is no response (clause 3.13).
func (c *Client2) Disconnect(reason sn2.Reason, expiry *uint32) error {
	p := &sn2.Packet{Type: sn2.DISCONNECT, Reason: reason, HasReason: reason != 0}
	if expiry != nil {
		p.HasSessionExpiry, p.SessionExpiry = true, *expiry
	}
	return c.Send(p)
}

// SearchGW2 sends a 2.0 SEARCHGW to addr and returns the first 2.0 GWINFO's
// gateway ID.
func SearchGW2(ctx context.Context, addr string) (byte, error) {
	to, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return 0, err
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if _, err := conn.WriteToUDP((&sn2.Packet{Type: sn2.SEARCHGW}).Encode(), to); err != nil {
		return 0, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(dl)
	} else {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	}
	buf := make([]byte, 512)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			return 0, err
		}
		if p, err := sn2.Decode(buf[:n]); err == nil && p.Type == sn2.GWINFO {
			return p.GwID, nil
		}
	}
}
