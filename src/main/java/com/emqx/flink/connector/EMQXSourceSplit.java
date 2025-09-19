package com.emqx.flink.connector;

import java.io.Serializable;

import org.apache.flink.api.connector.source.SourceSplit;

class EMQXSourceSplit implements SourceSplit, Serializable {
    private int id;

    EMQXSourceSplit(int id) {
        this.id = id;
    }

    public String splitId() {
        return String.valueOf(id);
    }

    @Override
    public String toString() {
        return String.format("EMQXSourceSplit(%d)", id);
    }
}
