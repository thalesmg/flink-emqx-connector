package com.emqx.flink.connector;

import org.apache.flink.api.connector.source.SourceSplit;

class EMQXSourceSplit implements SourceSplit {
    public String splitId() {
        return "dummy";
    }
}
