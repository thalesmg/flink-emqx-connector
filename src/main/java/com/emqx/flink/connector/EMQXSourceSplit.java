package com.emqx.flink.connector;

import java.io.Serializable;

import org.apache.flink.api.connector.source.SourceSplit;

class EMQXSourceSplit implements SourceSplit, Serializable {
    private String clientid;

    EMQXSourceSplit(String clientid) {
        this.clientid = clientid;
    }

    public String getClientid() {
        return this.clientid;
    }

    public String splitId() {
        return this.clientid;
    }

    @Override
    public String toString() {
        return String.format("EMQXSourceSplit(%s)", clientid);
    }
}
