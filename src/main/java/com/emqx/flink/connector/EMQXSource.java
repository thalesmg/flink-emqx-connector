package com.emqx.flink.connector;

// flink-core
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;
import org.apache.flink.util.Preconditions;
// flink-table-common;
import org.apache.flink.table.data.RowData;
// org.eclipse.paho.mqttv5.client;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class EMQXSource<OUT>
        implements Source<EMQXMessage<OUT>, EMQXSourceSplit, EMQXCheckpoint>, ResultTypeQueryable<EMQXMessage<OUT>> {
    private static final Logger LOG = LoggerFactory.getLogger(EMQXSplitEnumerator.class);

    private String brokerUri;
    private String clientid;
    private String groupName;
    private String topicFilter;
    private int qos;
    private DeserializationSchema<OUT> deserializer;

    public EMQXSource(String brokerUri, String clientid, String groupName, String topicFilter, int qos,
            DeserializationSchema<OUT> deserializer) {
        Preconditions.checkArgument(0 <= qos && qos <= 2, "invalid qos: %", qos);
        // TODO: validate group name and clientid
        this.brokerUri = brokerUri;
        this.clientid = clientid;
        this.groupName = groupName;
        this.topicFilter = topicFilter;
        this.qos = qos;
        this.deserializer = deserializer;
    }

    @Override
    public SplitEnumerator<EMQXSourceSplit, EMQXCheckpoint> createEnumerator(
            SplitEnumeratorContext<EMQXSourceSplit> arg0) throws Exception {
        return new EMQXSplitEnumerator();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SimpleVersionedSerializer<EMQXSourceSplit> getSplitSerializer() {
        return new SimpleSerializer<EMQXSourceSplit>();
    }

    @Override
    public SourceReader<EMQXMessage<OUT>, EMQXSourceSplit> createReader(SourceReaderContext context) throws Exception {
        int subTaskId = context.getIndexOfSubtask();
        String newClientid = clientid + subTaskId;
        LOG.debug("Starting Source Reader; clientid: {}; group name: {}", newClientid, groupName);
        return new EMQXSourceReader<>(brokerUri, newClientid, groupName, topicFilter, qos, deserializer);
    }

    @Override
    public SimpleVersionedSerializer<EMQXCheckpoint> getEnumeratorCheckpointSerializer() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TypeInformation<EMQXMessage<OUT>> getProducedType() {
        // return deserializer.getProducedType();
        return TypeInformation.of(new TypeHint<EMQXMessage<OUT>>(){});
    }

    @Override
    public SplitEnumerator<EMQXSourceSplit, EMQXCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<EMQXSourceSplit> enumContext, EMQXCheckpoint checkpoint) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }
}
