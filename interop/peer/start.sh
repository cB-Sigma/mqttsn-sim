#!/bin/sh
# Copyright 2026 Abhinav Gautam
# SPDX-License-Identifier: Apache-2.0
#
# Entry point of the interop image. "gateway" (the default) starts
# Mosquitto on 127.0.0.1:1883 and the Paho MQTT-SN gateway on UDP 10000;
# anything else is run as a command (a Paho sample client, say).
set -e
if [ "$#" -gt 0 ] && [ "$1" != "gateway" ]; then
	exec "$@"
fi
mosquitto -p 1883 -d
for i in 1 2 3 4 5 6 7 8 9 10; do
	if mosquitto_pub -h 127.0.0.1 -p 1883 -t sigma/ready -m 1 2>/dev/null; then
		break
	fi
	sleep 0.2
done
echo "PEER READY"
exec MQTT-SNGateway -f /etc/paho/gateway.conf
