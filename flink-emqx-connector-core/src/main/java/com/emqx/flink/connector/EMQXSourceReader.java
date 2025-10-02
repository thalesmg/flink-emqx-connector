package com.emqx.flink.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;

public class EMQXSourceReader<OUT> implements SourceReader<EMQXMessage<OUT>, EMQXSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(EMQXSourceReader.class);

    private Queue<Tuple2<Mqtt5Publish, EMQXMessage<OUT>>> queue = new ConcurrentLinkedQueue<>();
    private Mqtt5AsyncClient client;
    private MultipleFuturesAvailabilityHelper availabilityHelper = new MultipleFuturesAvailabilityHelper(1);

    private SourceReaderContext context;
    private String brokerHost;
    private int brokerPort;
    private String clientid;
    private String groupName;
    private String topicFilter;
    private int qos;
    private DeserializationSchema<OUT> deserializer;
    private List<EMQXSourceSplit> splits = new ArrayList<>();
    private final List<Mqtt5Publish> msgsToAck = new ArrayList<>();
    private final SortedMap<Long, List<Mqtt5Publish>> checkpointsToMsgsToAck;

    EMQXSourceReader(SourceReaderContext context, String brokerHost, int brokerPort, String clientid, String groupName,
            String topicFilter, int qos,
            DeserializationSchema<OUT> deserializer) {
        this.context = context;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.clientid = clientid;
        this.groupName = groupName;
        this.topicFilter = topicFilter;
        this.qos = qos;
        this.deserializer = deserializer;
        this.checkpointsToMsgsToAck = Collections.synchronizedSortedMap(new TreeMap<>());
    }

    void consumeMessage(Mqtt5Publish publish) {
        LOG.debug("received message: {}", publish);
        OUT decoded;
        try {
            decoded = deserializer.deserialize(publish.getPayloadAsBytes());
            EMQXMessage<OUT> emqxMessage = new EMQXMessage<>(
                    publish.getTopic().toString(), publish.getQos().getCode(), publish.isRetain(),
                    publish.getUserProperties(),
                    decoded);
            queue.add(new Tuple2(publish, emqxMessage));
            CompletableFuture<Void> cachedPreviousFuture = (CompletableFuture<Void>) availabilityHelper
                    .getAvailableFuture();
            cachedPreviousFuture.complete(null);
        } catch (IOException e) {
            LOG.error("error deserializing mqtt message", e);
        }
    }

    Mqtt5AsyncClient startClient2(String host, int port, String clientid, String groupName, String topicFilter, int qos,
            DeserializationSchema<OUT> deserializer)
            throws InterruptedException, ExecutionException {
        Mqtt5AsyncClient client = Mqtt5Client.builder()
                .serverHost(host)
                .serverPort(port)
                .identifier(clientid)
                .automaticReconnectWithDefaultConfig()
                .buildAsync();
        // "Fallback" flow for resuming sessions
        client.publishes(MqttGlobalPublishFilter.REMAINING, this::consumeMessage, true);
        // TODO: make session-expiry-interval a parameter
        LOG.info("connecting mqtt client for {}", clientid);
        client.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(60)
                .send()
                .thenAccept((connack) -> {
                    String subTopic = "$share/" + groupName + "/" + topicFilter;
                    if (!connack.isSessionPresent()) {
                        LOG.info("new session: subscribing to topic filter {}", subTopic);
                        client.subscribeWith()
                                .topicFilter(subTopic)
                                .qos(MqttQos.fromCode(qos))
                                .callback(this::consumeMessage)
                                .manualAcknowledgement(true)
                                .send()
                                .join();
                    } else {
                        LOG.info("session already present; will NOT subscribe explicitly");
                    }
                    LOG.info("mqtt client for {} connected", clientid);
                });
        return client;
    }

    @Override
    public void start() {
        // context.sendSplitRequest();
        try {
            client = startClient2(brokerHost, brokerPort, clientid, groupName, topicFilter, qos, deserializer);
        } catch (Exception e) {
            LOG.error("Error starting client {}", clientid, e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("stopping client");
        if (client != null) {
            client.disconnect().join();
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
        Tuple2<Mqtt5Publish, EMQXMessage<OUT>> tuple = queue.poll();
        if (tuple == null) {
            return InputStatus.NOTHING_AVAILABLE;
        } else {
            output.collect(tuple.f1);
            msgsToAck.add(tuple.f0);
            return InputStatus.MORE_AVAILABLE;
        }
    }

    @Override
    public List<EMQXSourceSplit> snapshotState(long checkpointId) {
        LOG.debug("snapshotState: checkpointId: {}; splits: {}; msgs to ack: {}",
                checkpointId, splits, msgsToAck);
        List<Mqtt5Publish> msgsToAckTmp = new ArrayList<>(this.msgsToAck);
        synchronized (checkpointsToMsgsToAck) {
            if (msgsToAckTmp.size() > 0) {
                checkpointsToMsgsToAck.put(checkpointId, msgsToAckTmp);
            }
        }
        msgsToAck.clear();
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("checkpoint complete: {}", checkpointId);
        // Subsume previous checkpoints.
        synchronized (checkpointsToMsgsToAck) {
            if (checkpointsToMsgsToAck.size() > 0) {
                SortedMap<Long, List<Mqtt5Publish>> sm = checkpointsToMsgsToAck.subMap(
                        checkpointsToMsgsToAck.firstKey(),
                        // need to guard against overflow?
                        checkpointId + 1);

                Iterator<Map.Entry<Long, List<Mqtt5Publish>>> iter = sm.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<Long, List<Mqtt5Publish>> entry = iter.next();
                    LOG.debug("acking {} messages for checkpoint {}", entry.getValue().size(), entry.getKey());
                    entry.getValue().forEach(Mqtt5Publish::acknowledge);
                    iter.remove();
                }
            }
        }
        SourceReader.super.notifyCheckpointComplete(checkpointId);
    }
}
