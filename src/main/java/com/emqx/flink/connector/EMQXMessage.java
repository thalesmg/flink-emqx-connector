package com.emqx.flink.connector;

import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

public class EMQXMessage<PAYLOAD> {
    public int id;
    public String topic;
    public int qos;
    public boolean retained;
    public MqttProperties properties;
    public PAYLOAD payload;

    public EMQXMessage(int id, String topic, int qos, boolean retained, MqttProperties properties, PAYLOAD payload) {
        this.id = id;
        this.topic = topic;
        this.qos = qos;
        this.retained = retained;
        this.properties = properties;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return String.format("EMQXMessage{id: %d, topic: %s, qos: %d, retained: %s, properties: %s, payload: %s}",
                id, topic, qos, retained, properties, payload);
    }
}
