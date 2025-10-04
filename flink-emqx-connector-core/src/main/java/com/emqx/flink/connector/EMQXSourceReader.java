package com.emqx.flink.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;

public class EMQXSourceReader<OUT> implements SourceReader<EMQXMessage<OUT>, EMQXSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(EMQXSourceReader.class);

    private Queue<EMQXMessage<OUT>> queue = new ConcurrentLinkedQueue<>();
    private MqttAsyncClient client;
    private MultipleFuturesAvailabilityHelper availabilityHelper = new MultipleFuturesAvailabilityHelper(1);

    private SourceReaderContext context;
    private String brokerHost;
    private int brokerPort;
    private String clientid;
    private String userName;
    private String password;
    private String groupName;
    private String topicFilter;
    private int qos;
    private DeserializationSchema<OUT> deserializer;
    private List<EMQXSourceSplit> splits = new ArrayList<>();

    EMQXSourceReader(SourceReaderContext context, String brokerHost, int brokerPort, String clientid, 
            String userName, String password, String groupName,
            String topicFilter, int qos,
            DeserializationSchema<OUT> deserializer) {
        this.context = context;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.clientid = clientid;
        this.userName = userName;
        this.password = password;
        this.groupName = groupName;
        this.topicFilter = topicFilter;
        this.qos = qos;
        this.deserializer = deserializer;
    }

    void consumeMessage(String topic, MqttMessage message) {
        LOG.debug("received message on topic: {}", topic);
        OUT decoded;
        try {
            decoded = deserializer.deserialize(message.getPayload());
            EMQXMessage<OUT> emqxMessage = new EMQXMessage<>(
                    topic, 
                    message.getQos(), 
                    message.isRetained(),
                    message.getProperties(),
                    decoded);
            
            queue.add(emqxMessage);
            
            CompletableFuture<Void> cachedPreviousFuture = (CompletableFuture<Void>) availabilityHelper
                    .getAvailableFuture();
            cachedPreviousFuture.complete(null);
        } catch (IOException e) {
            LOG.error("error deserializing mqtt message", e);
        }
    }

    MqttAsyncClient startClient(String host, int port, String clientid, String userName, String password, 
            String groupName, String topicFilter, int qos,
            DeserializationSchema<OUT> deserializer) throws MqttException {
        
        String broker = String.format("tcp://%s:%d", host, port);
        MqttAsyncClient client = new MqttAsyncClient(broker, clientid);
        
        // Set callback for incoming messages
        client.setCallback(new MqttCallback() {
            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                LOG.warn("Client {} disconnected: {}", clientid, disconnectResponse);
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
                LOG.error("MQTT error occurred for client {}", clientid, exception);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                consumeMessage(topic, message);
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
                // Not used for consumer
            }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                LOG.info("Connect complete for client {}. Reconnect: {}", clientid, reconnect);
                if (reconnect) {
                    // Resubscribe after reconnection
                    try {
                        subscribeToTopic(client, groupName, topicFilter, qos);
                    } catch (MqttException e) {
                        LOG.error("Error resubscribing after reconnect", e);
                    }
                }
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) {
                // Not used
            }
        });

        // Configure connection options
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setCleanStart(false);
        options.setSessionExpiryInterval(60L);
        options.setAutomaticReconnect(true);
        
        // Only set auth if username and password are provided
        if (userName != null && !userName.isEmpty() && password != null && !password.isEmpty()) {
            options.setUserName(userName);
            options.setPassword(password.getBytes());
        }

        LOG.info("Connecting MQTT client for {}", clientid);
        
        // Connect and subscribe
        client.connect(options).waitForCompletion();
        LOG.info("MQTT client for {} connected", clientid);
        
        // Subscribe to topic
        subscribeToTopic(client, groupName, topicFilter, qos);
        
        return client;
    }

    private void subscribeToTopic(MqttAsyncClient client, String groupName, String topicFilter, int qos) 
            throws MqttException {
        String subTopic = "$share/" + groupName + "/" + topicFilter;
        LOG.info("Subscribing to topic filter {}", subTopic);
        client.subscribe(subTopic, qos).waitForCompletion();
        LOG.info("Subscribed to topic filter {}", subTopic);
    }

    @Override
    public void start() {
        // Request a split assignment from the enumerator
        context.sendSplitRequest();
        
        try {
            client = startClient(brokerHost, brokerPort, clientid, userName, password, groupName, topicFilter, qos, deserializer);
        } catch (Exception e) {
            LOG.error("Error starting client {}", clientid, e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Stopping client");
        if (client != null && client.isConnected()) {
            client.disconnect().waitForCompletion();
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
        EMQXMessage<OUT> message = queue.poll();
        if (message == null) {
            return InputStatus.NOTHING_AVAILABLE;
        } else {
            output.collect(message);
            return InputStatus.MORE_AVAILABLE;
        }
    }

    @Override
    public List<EMQXSourceSplit> snapshotState(long checkpointId) {
        LOG.debug("snapshotState: checkpointId: {}; splits: {}", checkpointId, splits);
        // With Paho MQTT v5, messages are automatically acknowledged
        // We rely on cleanStart=false and session persistence for message delivery guarantees
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("checkpoint complete: {}", checkpointId);
        // With Paho MQTT v5 and cleanStart=false, messages are automatically acknowledged
        // The MQTT broker maintains the session and ensures delivery
        SourceReader.super.notifyCheckpointComplete(checkpointId);
    }
}
