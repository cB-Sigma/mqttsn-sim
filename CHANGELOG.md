# Changelog

## v0.1.0 (2026-10-11)

First published release: the MQTT-SN 1.2 client (`sim.Client`), the
MQTT-SN 2.0 client (`sim.Client2`) and `cmd/mqttsnsim`, a simulated
sensor.

Changes made while testing the 1.2 client with Eclipse Paho's MQTT-SN
gateway before the first release:

- `Client.Connect` returns `ErrDisconnected` at once when the gateway
  answers the connection exchange with DISCONNECT, instead of waiting for
  a CONNACK until the context ends.
