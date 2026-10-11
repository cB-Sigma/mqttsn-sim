# Provenance

## How the code was written

Written for SigmaM2M, a oneM2M platform, in Go, by the author of the
mqttsn module and from the same specifications (MQTT-SN Protocol
Specification Version 1.2, IBM, 2013; OASIS MQTT-SN Version 2.0,
Committee Specification Draft 01, 05 February 2026; RFC 5802 and RFC
7677 for SCRAM). No code was ported or copied from another
implementation. The clients were first written next to SigmaM2M's
MQTT-SN gateway to test it; they moved into the mqttsn module on
2026-10-06 and out of it into this module on 2026-10-11 (with `git mv`),
so the codec library does not carry its simulators. The unit tests in
sim/sim_test.go and the interop tests are new.

On 2026-10-11 the 1.2 client was run through the Eclipse Paho MQTT-SN
gateway (interop/). One change came from it: `Connect` returns
`ErrDisconnected` when a gateway answers the connection exchange with
DISCONNECT.

## Sources

| Area (files) | Followed |
|---|---|
| 1.2 client (sim/sim.go) | MQTT-SN 1.2, clause 6 (the client side of each procedure) |
| 2.0 client (sim/client2.go) | the 2.0 draft, client side; SCRAM through iotcrypto's sasl package |
| Command (cmd/mqttsnsim) | none (flags are our own) |
| Interop peers (interop/peer) | Paho's gateway.conf and predefinedTopic.conf formats and its sample clients' command lines, read to write our settings and start script |

No specification text is reproduced; comments cite clause numbers in our
own words.

## Tests

| Test | Source |
|---|---|
| sim/sim_test.go, cmd/mqttsnsim/main_test.go | a minimal gateway written in the test |
| interop/ | the Eclipse Paho MQTT-SN gateway at commit e1e1b73 with Mosquitto |

Client IDs, topics, keys and passwords are made-up demo values.
