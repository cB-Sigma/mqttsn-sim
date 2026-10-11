# mqttsn-sim

MQTT-SN 1.2 and 2.0 clients over UDP in Go, for testing MQTT-SN gateways without devices.

[![Go Reference](https://pkg.go.dev/badge/github.com/cB-Sigma/mqttsn-sim.svg)](https://pkg.go.dev/github.com/cB-Sigma/mqttsn-sim) [![test](https://github.com/cB-Sigma/mqttsn-sim/actions/workflows/test.yml/badge.svg)](https://github.com/cB-Sigma/mqttsn-sim/actions/workflows/test.yml) [![Licence: Apache-2.0](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE) [![Go](https://img.shields.io/badge/go-1.26.0-00ADD8.svg)](go.mod)

The 1.2 client searches for a gateway, connects with a will, registers
topics, publishes at QoS -1 to 2 on normal, predefined and short topics,
subscribes, sleeps and wakes up, and can misbehave on purpose (go silent,
refuse publishes as congested). The 2.0 client speaks the OASIS MQTT-SN
2.0 Committee Specification Draft 01, with SCRAM authentication and the
Protection Encapsulation. A small command runs a simulated sensor. Both
are built on the codecs of [mqttsn](https://github.com/cB-Sigma/mqttsn).
Written for SigmaM2M, a oneM2M platform, to test its MQTT-SN gateway; it
has no dependency on it.

## What is simulated

| Package | What it does |
|---|---|
| `sim` (1.2) | `Dial`, `DialFrom`, `SearchGW`; `Client.Connect` (with a `Will`), `Register`, `Publish`, `Subscribe`, `Unsubscribe`, `Ping`, `Sleep`, `Wake`, `Disconnect`, `Request`, `Send`; deliveries on `Messages`; `Silence`, `Congest` |
| `sim` (2.0) | `Dial2`; `Client2.Connect`, `ConnectSCRAM`, `ReauthSCRAM`, `Register`, `Publish`, `PubWOS`, `Subscribe`, `Unsubscribe`, `Ping`, `Sleep`, `Wake`, `Disconnect`; `Protect` for the Protection Encapsulation; deliveries on `Messages`; `Silence`, `RefuseRegister` |
| `cmd/mqttsnsim` | a sensor that connects to a gateway and publishes readings (`-v2` for 2.0) |

Requests are sequential: one call at a time per client. Not simulated:
a gateway, DTLS, the forwarder encapsulation.

## Install

```
go get github.com/cB-Sigma/mqttsn-sim@latest
```

Requires Go 1.26.0 or later. The command:

```
go install github.com/cB-Sigma/mqttsn-sim/cmd/mqttsnsim@latest
```

## Command line

```
mqttsnsim -gw 127.0.0.1:1884 -id sensor-1 -topic sensors/sensor-1/temperature
mqttsnsim -gw 127.0.0.1:1884 -v2 -qos 2 -count 10 -every 1s
```

| Flag | Default | Meaning |
|---|---|---|
| `-gw` | `127.0.0.1:1884` | gateway UDP address |
| `-id` | `mqttsnsim-1` | client ID |
| `-topic` | `sensors/mqttsnsim-1/temperature` | topic the readings are published on |
| `-sub` | | topic filter to subscribe to (none when empty) |
| `-every` | `5s` | interval between readings |
| `-qos` | `1` | QoS of the readings (0, 1 or 2) |
| `-count` | `0` | stop after this many readings (0: run until interrupted) |
| `-v2` | off | speak MQTT-SN 2.0 (draft CSD01) instead of 1.2 |

## In Go tests

```go
c, _ := sim.Dial("127.0.0.1:1884", "sensor-1")
defer c.Close()
_ = c.Connect(ctx, time.Minute, true, &sim.Will{Topic: "sensors/sensor-1/lost", Msg: []byte("gone")})
id, _ := c.Register(ctx, "sensors/sensor-1/temperature")
_ = c.Publish(ctx, mqttsn.TopicNormal, id, 1, false, []byte("21.5"))
```

## Demo values

Every client ID, topic, key and password in this module is a made-up
demo value.

## Documentation

Reference: https://pkg.go.dev/github.com/cB-Sigma/mqttsn-sim

## Testing

```
go vet ./...
go test -race ./...
```

The unit tests run the clients against a minimal in-test gateway. The
fuller tests of both clients run against SigmaM2M's gateway, written by
the same author, in SigmaM2M.

`interop/` is a nested module with its own go.mod. It builds the Eclipse
Paho MQTT-SN gateway (paho.mqtt-sn.embedded-c, EPL-2.0 / EDL-1.0) at a
pinned commit next to a Mosquitto broker in a Debian container
(`interop/peer`) and runs the 1.2 client through it. It is skipped unless
`SIGMA_MQTTSN_INTEROP=1` is set and Docker is running:

```
cd interop && SIGMA_MQTTSN_INTEROP=1 go test -v ./...
```

### Interop tested against

On 2026-10-11, Docker Desktop 29.4.3 on arm64, Paho MQTT-SN gateway 1.5.1
(commit e1e1b73) and Mosquitto 2.0 from Debian bookworm, reached through
a container's UDP port:

- Publish: connect, REGISTER, PUBLISH at QoS 0, 1 and 2, on a short topic
  name, on a predefined topic ID and retained; mosquitto_sub on the
  broker received each on the right topic, and a later subscriber got
  the retained one; PINGREQ and DISCONNECT answered. Passed.
- Subscribe: a wildcard filter at QoS 1, a short topic name at QoS 2 and
  a predefined topic ID; messages mosquitto_pub sent arrived with the
  gateway's REGISTER for the new topic and at the granted QoS; nothing
  arrived after UNSUBSCRIBE. Passed.
- Sleep: messages sent while the client slept were delivered when it
  woke up. Passed. Paho sent the closing PINGRESP only when it had to
  REGISTER a topic during the wake-up; otherwise it sent none, so
  `Wake` timed out although the messages had arrived (a Paho defect;
  the test records it).
- Will: Paho asked for the will topic and then discarded the WILLTOPIC
  and answered DISCONNECT, because its client state is still
  "disconnected" until it has sent CONNECT to the broker; no will can be
  set through this Paho version. The test is skipped with that reason.
  `Connect` now returns `ErrDisconnected` at once in that case.
- Not tested against Paho: QoS -1 (Paho accepts it only from addresses
  listed in its client file, which a container's NAT hides), SEARCHGW
  (Paho answers by multicast), the 2.0 client (Paho implements 1.2
  only).
- MQTT-SN 2.0: no independent implementation of draft CSD01 was found
  (see mqttsn's README); the 2.0 client has been run only against
  SigmaM2M's gateway, by the same author.

## Related repositories

- [mqttsn](https://github.com/cB-Sigma/mqttsn): the MQTT-SN 1.2 and 2.0
  codecs.
- [iotcrypto](https://github.com/cB-Sigma/iotcrypto): SCRAM for the 2.0
  client.

## Status

v0.x. The API may change before v1.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [SECURITY.md](SECURITY.md).

## Licence

Apache-2.0 (see [LICENSE](LICENSE) and [NOTICE](NOTICE)). Copyright 2026
Abhinav Gautam.
