package com.emqx.flink.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
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

public class EMQXSourceReader<OUT> implements SourceReader<EMQXMessage<OUT>, EMQXSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(EMQXSourceReader.class);

    private Queue<EMQXMessage<OUT>> queue = new ConcurrentLinkedQueue<>();
    private MqttClient client;
    private MultipleFuturesAvailabilityHelper availabilityHelper = new MultipleFuturesAvailabilityHelper(1);

    private SourceReaderContext context;
    private String brokerUri;
    private String clientid;
    private String groupName;
    private String topicFilter;
    private int qos;
    private DeserializationSchema<OUT> deserializer;
    private List<EMQXSourceSplit> splits = new ArrayList<>();
    // TODO: just testing
    private List<Integer> msgIdsToAck = new ArrayList<>();

    EMQXSourceReader(SourceReaderContext context, String brokerUri, String clientid, String groupName,
            String topicFilter, int qos,
            DeserializationSchema<OUT> deserializer) {
        this.context = context;
        this.brokerUri = brokerUri;
        this.clientid = clientid;
        this.groupName = groupName;
        this.topicFilter = topicFilter;
        this.qos = qos;
        this.deserializer = deserializer;
    }

    MqttClient startClient(String brokerUri, String clientid, String groupName, String topicFilter, int qos,
            DeserializationSchema<OUT> deserializer) throws MqttSecurityException, MqttException {
        LOG.debug("Starting Source Reader with clientid {}", clientid);
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
                LOG.debug("received message: topic: {}; {}", topic, message.toDebugString());
                OUT decoded = deserializer.deserialize(message.getPayload());
                EMQXMessage<OUT> emqxMessage = new EMQXMessage<>(
                        message.getId(), topic, message.getQos(), message.isRetained(), message.getProperties(),
                        decoded);
                queue.add(emqxMessage);
                CompletableFuture<Void> cachedPreviousFuture = (CompletableFuture<Void>) availabilityHelper
                        .getAvailableFuture();
                cachedPreviousFuture.complete(null);
                // FIXME: remove!! only testing unacked replay!!
                Object lock = new Object();
                lock.wait();
            }
        };
        connOpts.setCleanStart(false);
        connOpts.setAutomaticReconnect(true);
        connOpts.setSessionExpiryInterval(60L);
        client.setCallback(callback);
        client.setManualAcks(true);
        client.connect(connOpts);
        String subTopic = "$share/" + groupName + "/" + topicFilter;
        client.subscribe(subTopic, qos);
        return client;
    }

    @Override
    public void start() {
        context.sendSplitRequest();
        try {
            client = startClient(brokerUri, clientid, groupName, topicFilter, qos, deserializer);
            LOG.info("started mqtt client for clientid {}", clientid);
        } catch (Exception e) {
            LOG.error("Error starting client: {}", e.getMessage(), e);
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
        LOG.debug("Adding splits for clientid {}; splits: {}", clientid, splits);
        this.splits.addAll(splits);
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
    public InputStatus pollNext(ReaderOutput<EMQXMessage<OUT>> output) throws Exception {
        EMQXMessage<OUT> value = queue.poll();
        if (value == null) {
            return InputStatus.NOTHING_AVAILABLE;
        } else {
            msgIdsToAck.add(value.id);
            output.collect(value);
            return InputStatus.MORE_AVAILABLE;
        }
    }

    @Override
    public List<EMQXSourceSplit> snapshotState(long checkpointId) {
        // TODO stub
        splits.forEach((split) -> split.putIds(msgIdsToAck));
        LOG.debug("snapshotState: checkpointId: {}; splits: {}; msg ids to ack: {}",
                checkpointId, splits, msgIdsToAck);
        msgIdsToAck.clear();
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // TODO Auto-generated method stub
        LOG.debug("checkpoint complete: {}", checkpointId);
        SourceReader.super.notifyCheckpointComplete(checkpointId);
    }
}
