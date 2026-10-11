# Changelog

## v0.2.0 (2026-10-12)

- The clients moved into the mqttsn module's `client` package (mqttsn
  v0.2.0); `sim.Client` and `sim.Client2` wrap them and keep their fault
  switches (`Silence`, `Congest`, `RefuseRegister`). The API is unchanged:
  `Delivery`, `Will`, `Protection`, `Connect2`, `Subscribe2` and the
  errors are aliases of the client package's.
- `StartGateway`: a simulated 1.2 gateway with an in-process broker and
  `Silence` and `DropNext`.

## v0.1.0 (2026-10-11)

First published release: the MQTT-SN 1.2 client (`sim.Client`), the
MQTT-SN 2.0 client (`sim.Client2`) and `cmd/mqttsnsim`, a simulated
sensor.

Changes made while testing the 1.2 client with Eclipse Paho's MQTT-SN
gateway before the first release:

- `Client.Connect` returns `ErrDisconnected` at once when the gateway
  answers the connection exchange with DISCONNECT, instead of waiting for
  a CONNACK until the context ends.
