package com.emqx.flink.connector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.connector.source.SourceSplit;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

class EMQXSourceSplit implements SourceSplit, Serializable {
    private String clientid;

    private List<Mqtt5Publish> messagesToCheckpoint;

    EMQXSourceSplit(String clientid) {
        this(clientid, Collections.emptyList());
    }

    EMQXSourceSplit(String clientid, List<Mqtt5Publish> messagesToCheckpoint) {
        this.clientid = clientid;
        this.messagesToCheckpoint = messagesToCheckpoint;
    }

    public void putMsgs(List<Mqtt5Publish> messagesToCheckpoint) {
        this.messagesToCheckpoint = new ArrayList<>(messagesToCheckpoint);
    }

    public String getClientid() {
        return this.clientid;
    }

    public List<Mqtt5Publish> getMsgs() {
        List<Mqtt5Publish> copy = new ArrayList<>(this.messagesToCheckpoint);
        return copy;
    }

    public String splitId() {
        return clientid;
    }

    @Override
    public String toString() {
        return String.format("EMQXSourceSplit(%s)[%s]", clientid, messagesToCheckpoint);
    }
}
