package com.emqx.flink.connector;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.connector.source.SplitEnumerator;

public class EMQXSplitEnumerator implements SplitEnumerator<EMQXSourceSplit, EMQXCheckpoint> {
    private static final Logger LOG = LoggerFactory.getLogger(EMQXSplitEnumerator.class);

    @Override
    public void start() {
        // TODO Auto-generated method stub
        LOG.debug("start");
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void addReader(int subTaskId) {
        // TODO Auto-generated method stub
        LOG.debug("addReader: {}", subTaskId);
    }

    @Override
    public void addSplitsBack(List splits, int subTaskId) {
        // TODO Auto-generated method stub

    }

    @Override
    public void handleSplitRequest(int subTaskId, @Nullable String requesterHostname) {
        // TODO Auto-generated method stub

    }

    @Override
    public EMQXCheckpoint snapshotState(long checkpointId) throws Exception {
        // TODO Auto-generated method stub
        return new EMQXCheckpoint();
    }
}
