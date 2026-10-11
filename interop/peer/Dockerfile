# Copyright 2026 Abhinav Gautam
# SPDX-License-Identifier: Apache-2.0
#
# Reference MQTT-SN peers for the interop tests: the Eclipse Paho MQTT-SN
# gateway and the Paho MQTTSNPacket sample clients
# (github.com/eclipse/paho.mqtt-sn.embedded-c, EPL-2.0 / EDL-1.0), built
# from source at a pinned commit, next to a Mosquitto broker from Debian.
#
#   docker build -t sigma-mqttsn-interop:paho-e1e1b73 .

FROM debian:bookworm-slim AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ cmake make git ca-certificates libssl-dev \
 && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/eclipse/paho.mqtt-sn.embedded-c /src \
 && git -C /src checkout e1e1b733e0fecef6bed92798d7f6d6440d9026f2
RUN cmake -S /src -B /build -DSENSORNET=udp \
 && make -C /build -j4 MQTTSNPacket MQTT-SNGateway pub0sub1 qos1pub qos0pub qos-1pub qos0pub_register \
 && mkdir -p /out \
 && cp /src/MQTTSNGateway/bin/MQTT-SNGateway /out/ \
 && find /build -type f -perm -u+x \( -name MQTT-SNGateway -o -name pub0sub1 -o -name qos1pub -o -name qos0pub -o -name qos-1pub -o -name qos0pub_register \) -exec cp {} /out/ \; \
 && find /build -name '*.so*' -exec cp -P {} /out/ \;

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends mosquitto mosquitto-clients libssl3 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
COPY gateway.conf clients.conf predefinedTopic.conf /etc/paho/
COPY start.sh /start.sh
RUN chmod 0755 /start.sh && ldconfig /usr/local/bin
ENV LD_LIBRARY_PATH=/usr/local/bin
ENTRYPOINT ["/start.sh"]
