package com.emqx.flink.connector;

import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSecurityException;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

public class EMQXSourceReader<OUT> implements SourceReader<OUT, EMQXSourceSplit> {
    private Queue<OUT> queue = new ConcurrentLinkedQueue<>();
    private MqttClient client;
    private MultipleFuturesAvailabilityHelper availabilityHelper = new MultipleFuturesAvailabilityHelper(1);

    private String brokerUri;
    private String clientid;
    private String topicFilter;
    private int qos;
    private DeserializationSchema<OUT> deserializer;

    EMQXSourceReader(String brokerUri, String clientid, String topicFilter, int qos,
            DeserializationSchema<OUT> deserializer) {
        this.brokerUri = brokerUri;
        this.clientid = clientid;
        this.topicFilter = topicFilter;
        this.qos = qos;
        this.deserializer = deserializer;
    }

    MqttClient startClient(String brokerUri, String clientid, String topicFilter, int qos,
            DeserializationSchema<OUT> deserializer) throws MqttSecurityException, MqttException {
        MqttClient client = new MqttClient(brokerUri, clientid, null);
        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        MqttCallback callback = new MqttCallback() {
            // TODO: add logging

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) {
            }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
            }

            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                OUT decoded = deserializer.deserialize(message.getPayload());
                queue.add(decoded);
                CompletableFuture<Void> cachedPreviousFuture = (CompletableFuture<Void>) availabilityHelper
                        .getAvailableFuture();
                cachedPreviousFuture.complete(null);
            }
        };
        connOpts.setCleanStart(false);
        connOpts.setAutomaticReconnect(true);
        client.setCallback(callback);
        client.connect(connOpts);
        client.subscribe(topicFilter, qos);
        return client;
    }

    @Override
    public void start() {
        try {
            client = startClient(brokerUri, clientid, topicFilter, qos, deserializer);
        } catch (Exception e) {
            // TODO: add logging
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.disconnect();
            client.close();
        }
    }

    @Override
    public void addSplits(List<EMQXSourceSplit> splits) {
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        availabilityHelper.resetToUnAvailable();
        return (CompletableFuture<Void>) availabilityHelper.getAvailableFuture();
    }

    @Override
    public void notifyNoMoreSplits() {
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OUT> output) throws Exception {
        OUT value = queue.poll();
        if (value == null) {
            return InputStatus.NOTHING_AVAILABLE;
        } else {
            output.collect(value);
            return InputStatus.MORE_AVAILABLE;
        }
    }

    @Override
    public List<EMQXSourceSplit> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }
}
