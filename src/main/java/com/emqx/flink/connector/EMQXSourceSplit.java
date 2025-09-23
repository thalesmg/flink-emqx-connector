package com.emqx.flink.connector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.connector.source.SourceSplit;

class EMQXSourceSplit implements SourceSplit, Serializable {
    private String clientid;

    private List<Integer> messageIdsToCheckpoint;

    EMQXSourceSplit(String clientid) {
        this(clientid, Collections.emptyList());
    }

    EMQXSourceSplit(String clientid, List<Integer> messageIdsToCheckpoint) {
        this.clientid = clientid;
        this.messageIdsToCheckpoint = messageIdsToCheckpoint;
    }

    public void putIds(List<Integer> messageIdsToCheckpoint) {
        this.messageIdsToCheckpoint = new ArrayList<>(messageIdsToCheckpoint);
    }

    public String getClientid() {
        return this.clientid;
    }

    public List<Integer> getIds() {
        List<Integer> copy = new ArrayList<>(this.messageIdsToCheckpoint);
        return copy;
    }

    public String splitId() {
        return clientid;
    }

    @Override
    public String toString() {
        return String.format("EMQXSourceSplit(%s)[%s]", clientid, messageIdsToCheckpoint);
    }
}
