// Package sim has MQTT-SN clients over UDP for tests and
// demonstrations: Client speaks MQTT-SN v1.2 and Client2 MQTT-SN 2.0
// (draft CSD01). Client finds a gateway, connects (with a will), registers
// topics, publishes at every QoS, subscribes, sleeps and wakes up.
// Requests are sequential: one call at a time per Client.
package sim

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cB-Sigma/mqttsn"
)

// Delivery is a message the gateway published to the client.
type Delivery struct {
	Topic   string // resolved name; for a short topic the two characters
	TopicID uint16
	IDType  byte
	QoS     int8
	Retain  bool
	Data    []byte
}

// Client is an MQTT-SN client.
type Client struct {
	ID string

	conn *net.UDPConn
	gw   *net.UDPAddr
	resp chan *mqttsn.Message
	msgs chan Delivery

	mu     sync.Mutex
	names  map[uint16]string // topic IDs the gateway or we registered
	nextID uint16
	// Ignore makes the client drop everything it receives (a device that
	// went away without saying so).
	ignore bool
	// congested is how many more QoS 1 or 2 PUBLISHes are answered with
	// PUBACK "rejected: congestion" instead of being taken.
	congested int
	done      chan struct{}
}

// Congest makes the client answer the next n QoS 1 or 2 PUBLISHes with
// PUBACK "rejected: congestion" (a client short of buffers).
func (c *Client) Congest(n int) {
	c.mu.Lock()
	c.congested = n
	c.mu.Unlock()
}

// Dial opens a client socket towards the gateway at addr (host:port).
func Dial(addr, clientID string) (*Client, error) { return DialFrom("", addr, clientID) }

// DialFrom is Dial from the local address local (host:port), as a device
// that is given an address another device used before (NAT, DHCP).
func DialFrom(local, addr, clientID string) (*Client, error) {
	gw, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	la := &net.UDPAddr{IP: gw.IP.To4(), Port: 0}
	if local != "" {
		if la, err = net.ResolveUDPAddr("udp", local); err != nil {
			return nil, err
		}
	}
	conn, err := net.ListenUDP("udp", la)
	if err != nil && local == "" {
		conn, err = net.ListenUDP("udp", nil)
	}
	if err != nil {
		return nil, err
	}
	c := &Client{
		ID: clientID, conn: conn, gw: gw,
		resp: make(chan *mqttsn.Message, 64), msgs: make(chan Delivery, 256),
		names: map[uint16]string{}, done: make(chan struct{}),
	}
	go c.read()
	return c, nil
}

// LocalAddr is the client's address.
func (c *Client) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// Close closes the socket without telling the gateway.
func (c *Client) Close() {
	_ = c.conn.Close()
	<-c.done
}

// Messages are the gateway's publishes to this client.
func (c *Client) Messages() <-chan Delivery { return c.msgs }

// Silence makes the client stop answering (and stop sending keep-alives,
// which it never sends by itself): the gateway will declare it lost.
func (c *Client) Silence() {
	c.mu.Lock()
	c.ignore = true
	c.mu.Unlock()
}

// Send writes a raw message to the gateway.
func (c *Client) Send(m *mqttsn.Message) error {
	_, err := c.conn.WriteToUDP(m.Encode(), c.gw)
	return err
}

func (c *Client) read() {
	defer close(c.done)
	buf := make([]byte, 65536)
	for {
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		m, err := mqttsn.Decode(buf[:n])
		if err != nil {
			continue
		}
		c.mu.Lock()
		ignore := c.ignore
		c.mu.Unlock()
		if ignore {
			continue
		}
		switch m.Type {
		case mqttsn.REGISTER:
			c.mu.Lock()
			c.names[m.TopicID] = m.TopicName
			c.mu.Unlock()
			_ = c.Send(&mqttsn.Message{Type: mqttsn.REGACK, TopicID: m.TopicID, MsgID: m.MsgID, ReturnCode: mqttsn.Accepted})
		case mqttsn.PUBLISH:
			c.onPublish(m)
		case mqttsn.PUBREL:
			_ = c.Send(&mqttsn.Message{Type: mqttsn.PUBCOMP, MsgID: m.MsgID})
		default:
			select {
			case c.resp <- m:
			default:
			}
		}
	}
}

func (c *Client) onPublish(m *mqttsn.Message) {
	if m.Flags.QoS > 0 {
		c.mu.Lock()
		congested := c.congested > 0
		if congested {
			c.congested--
		}
		c.mu.Unlock()
		if congested {
			_ = c.Send(&mqttsn.Message{Type: mqttsn.PUBACK, TopicID: m.TopicID, MsgID: m.MsgID, ReturnCode: mqttsn.RejectedCongested})
			return
		}
	}
	d := Delivery{TopicID: m.TopicID, IDType: m.Flags.TopicIDType, QoS: m.Flags.QoS, Retain: m.Flags.Retain, Data: m.Data}
	switch m.Flags.TopicIDType {
	case mqttsn.TopicShort:
		d.Topic = mqttsn.ShortName(m.TopicID)
	default:
		c.mu.Lock()
		d.Topic = c.names[m.TopicID]
		c.mu.Unlock()
	}
	switch m.Flags.QoS {
	case 1:
		_ = c.Send(&mqttsn.Message{Type: mqttsn.PUBACK, TopicID: m.TopicID, MsgID: m.MsgID, ReturnCode: mqttsn.Accepted})
	case 2:
		_ = c.Send(&mqttsn.Message{Type: mqttsn.PUBREC, MsgID: m.MsgID})
	}
	select {
	case c.msgs <- d:
	default:
	}
}

// await waits for a response satisfying ok, discarding others.
func (c *Client) await(ctx context.Context, ok func(*mqttsn.Message) bool) (*mqttsn.Message, error) {
	for {
		select {
		case m := <-c.resp:
			if ok(m) {
				return m, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *Client) request(ctx context.Context, m *mqttsn.Message, want mqttsn.MsgType, msgID uint16) (*mqttsn.Message, error) {
	for attempt := 0; ; attempt++ {
		if err := c.Send(m); err != nil {
			return nil, err
		}
		actx, cancel := context.WithTimeout(ctx, 2*time.Second)
		r, err := c.await(actx, func(r *mqttsn.Message) bool {
			return r.Type == want && (msgID == 0 || r.MsgID == msgID)
		})
		cancel()
		if err == nil {
			return r, nil
		}
		if ctx.Err() != nil || attempt >= 3 {
			return nil, fmt.Errorf("no %s from the gateway: %w", want, err)
		}
		if m.Type == mqttsn.PUBLISH {
			m.Flags.DUP = true
		}
	}
}

func (c *Client) msgID() uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	if c.nextID == 0 {
		c.nextID = 1
	}
	return c.nextID
}

// Will is a last will for Connect.
type Will struct {
	Topic  string
	Msg    []byte
	QoS    int8
	Retain bool
}

// Connect connects with keep-alive duration, answering the will requests.
func (c *Client) Connect(ctx context.Context, keepAlive time.Duration, clean bool, w *Will) error {
	m := &mqttsn.Message{Type: mqttsn.CONNECT, Flags: mqttsn.Flags{CleanSession: clean, Will: w != nil},
		Duration: uint16(keepAlive / time.Second), ClientID: c.ID}
	if err := c.Send(m); err != nil {
		return err
	}
	for {
		r, err := c.await(ctx, func(r *mqttsn.Message) bool {
			return r.Type == mqttsn.CONNACK || r.Type == mqttsn.WILLTOPICREQ || r.Type == mqttsn.WILLMSGREQ || r.Type == mqttsn.DISCONNECT
		})
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		switch r.Type {
		case mqttsn.DISCONNECT:
			return fmt.Errorf("connect: %w", ErrDisconnected)
		case mqttsn.WILLTOPICREQ:
			_ = c.Send(&mqttsn.Message{Type: mqttsn.WILLTOPIC, Flags: mqttsn.Flags{QoS: w.QoS, Retain: w.Retain}, TopicName: w.Topic})
		case mqttsn.WILLMSGREQ:
			_ = c.Send(&mqttsn.Message{Type: mqttsn.WILLMSG, Data: w.Msg})
		case mqttsn.CONNACK:
			if r.ReturnCode != mqttsn.Accepted {
				return fmt.Errorf("connect refused: return code %d", r.ReturnCode)
			}
			return nil
		}
	}
}

// Register registers a topic name and returns its ID.
func (c *Client) Register(ctx context.Context, topic string) (uint16, error) {
	id := c.msgID()
	r, err := c.request(ctx, &mqttsn.Message{Type: mqttsn.REGISTER, MsgID: id, TopicName: topic}, mqttsn.REGACK, id)
	if err != nil {
		return 0, err
	}
	if r.ReturnCode != mqttsn.Accepted {
		return 0, fmt.Errorf("register %s refused: return code %d", topic, r.ReturnCode)
	}
	c.mu.Lock()
	c.names[r.TopicID] = topic
	c.mu.Unlock()
	return r.TopicID, nil
}

// ErrRejected is returned when the gateway answers with a return code.
var ErrRejected = errors.New("rejected by the gateway")

// ErrDisconnected is returned by Connect when the gateway answers the
// connection exchange with DISCONNECT instead of CONNACK.
var ErrDisconnected = errors.New("the gateway sent DISCONNECT")

// Publish publishes data on a topic (idType normal, predefined or short) at
// QoS -1, 0, 1 or 2, completing the acknowledgement exchange.
func (c *Client) Publish(ctx context.Context, idType byte, topicID uint16, qos int8, retain bool, data []byte) error {
	m := &mqttsn.Message{Type: mqttsn.PUBLISH, Flags: mqttsn.Flags{QoS: qos, Retain: retain, TopicIDType: idType}, TopicID: topicID, Data: data}
	switch qos {
	case -1, 0:
		return c.Send(m)
	case 1:
		m.MsgID = c.msgID()
		r, err := c.request(ctx, m, mqttsn.PUBACK, m.MsgID)
		if err != nil {
			return err
		}
		if r.ReturnCode != mqttsn.Accepted {
			return fmt.Errorf("%w: return code %d", ErrRejected, r.ReturnCode)
		}
		return nil
	case 2:
		m.MsgID = c.msgID()
		r, err := c.awaitAny(ctx, m, m.MsgID)
		if err != nil {
			return err
		}
		if r.Type == mqttsn.PUBACK {
			return fmt.Errorf("%w: return code %d", ErrRejected, r.ReturnCode)
		}
		_, err = c.request(ctx, &mqttsn.Message{Type: mqttsn.PUBREL, MsgID: m.MsgID}, mqttsn.PUBCOMP, m.MsgID)
		return err
	}
	return fmt.Errorf("QoS %d is not -1, 0, 1 or 2", qos)
}

// awaitAny sends m and waits for PUBREC or a PUBACK refusal.
func (c *Client) awaitAny(ctx context.Context, m *mqttsn.Message, id uint16) (*mqttsn.Message, error) {
	if err := c.Send(m); err != nil {
		return nil, err
	}
	actx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.await(actx, func(r *mqttsn.Message) bool {
		return (r.Type == mqttsn.PUBREC || r.Type == mqttsn.PUBACK) && r.MsgID == id
	})
}

// Subscribe subscribes to a topic name (idType normal, wildcards allowed),
// a predefined topic ID or a short topic name; it returns the topic ID and
// granted QoS from SUBACK.
func (c *Client) Subscribe(ctx context.Context, idType byte, topic string, topicID uint16, qos int8) (uint16, int8, error) {
	id := c.msgID()
	m := &mqttsn.Message{Type: mqttsn.SUBSCRIBE, Flags: mqttsn.Flags{QoS: qos, TopicIDType: idType}, MsgID: id, TopicName: topic, TopicID: topicID}
	if idType == mqttsn.TopicShort {
		m.TopicID, _ = mqttsn.ShortTopic(topic)
		m.TopicName = ""
	}
	r, err := c.request(ctx, m, mqttsn.SUBACK, id)
	if err != nil {
		return 0, 0, err
	}
	if r.ReturnCode != mqttsn.Accepted {
		return 0, 0, fmt.Errorf("%w: return code %d", ErrRejected, r.ReturnCode)
	}
	if idType == mqttsn.TopicNormal && r.TopicID != 0 {
		c.mu.Lock()
		c.names[r.TopicID] = topic
		c.mu.Unlock()
	}
	return r.TopicID, r.Flags.QoS, nil
}

// Unsubscribe removes a subscription by topic name.
func (c *Client) Unsubscribe(ctx context.Context, topic string) error {
	id := c.msgID()
	_, err := c.request(ctx, &mqttsn.Message{Type: mqttsn.UNSUBSCRIBE, MsgID: id, TopicName: topic}, mqttsn.UNSUBACK, id)
	return err
}

// Request sends m (retransmitting it like any request) and returns the
// gateway's first answer of type want.
func (c *Client) Request(ctx context.Context, m *mqttsn.Message, want mqttsn.MsgType) (*mqttsn.Message, error) {
	return c.request(ctx, m, want, 0)
}

// Ping sends a PINGREQ (without client ID) and waits for PINGRESP.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.request(ctx, &mqttsn.Message{Type: mqttsn.PINGREQ}, mqttsn.PINGRESP, 0)
	return err
}

// Sleep sends DISCONNECT with a sleep duration.
func (c *Client) Sleep(ctx context.Context, d time.Duration) error {
	_, err := c.request(ctx, &mqttsn.Message{Type: mqttsn.DISCONNECT, HasDuration: true, Duration: uint16(d / time.Second)}, mqttsn.DISCONNECT, 0)
	return err
}

// Wake sends a PINGREQ with the client ID and waits for PINGRESP, which
// the gateway sends after the messages it queued (read them from
// Messages).
func (c *Client) Wake(ctx context.Context) error {
	if err := c.Send(&mqttsn.Message{Type: mqttsn.PINGREQ, ClientID: c.ID}); err != nil {
		return err
	}
	actx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := c.await(actx, func(r *mqttsn.Message) bool { return r.Type == mqttsn.PINGRESP })
	return err
}

// Disconnect ends the session (no will).
func (c *Client) Disconnect(ctx context.Context) error {
	_, err := c.request(ctx, &mqttsn.Message{Type: mqttsn.DISCONNECT}, mqttsn.DISCONNECT, 0)
	return err
}

// SearchGW broadcasts (or sends) SEARCHGW to addr and returns the first
// GWINFO's gateway ID and sender.
func SearchGW(ctx context.Context, addr string, radius byte) (byte, net.Addr, error) {
	to, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return 0, nil, err
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return 0, nil, err
	}
	defer conn.Close()
	if _, err := conn.WriteToUDP((&mqttsn.Message{Type: mqttsn.SEARCHGW, Radius: radius}).Encode(), to); err != nil {
		return 0, nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(dl)
	} else {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	}
	buf := make([]byte, 512)
	for {
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			return 0, nil, err
		}
		if m, err := mqttsn.Decode(buf[:n]); err == nil && m.Type == mqttsn.GWINFO {
			return m.GwID, from, nil
		}
	}
}
