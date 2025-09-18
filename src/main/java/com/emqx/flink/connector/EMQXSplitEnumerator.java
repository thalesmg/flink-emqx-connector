package com.emqx.flink.connector;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

public class EMQXSplitEnumerator implements SplitEnumerator<EMQXSourceSplit, EMQXCheckpoint> {
    private static final Logger LOG = LoggerFactory.getLogger(EMQXSplitEnumerator.class);

    private SplitEnumeratorContext<EMQXSourceSplit> context;

    EMQXSplitEnumerator(SplitEnumeratorContext<EMQXSourceSplit> context) {
        this.context = context;
    }

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
        LOG.debug("handleSplitRequest: {}, {}", subTaskId, requesterHostname);
        context.assignSplit(new EMQXSourceSplit(subTaskId), subTaskId);
    }

    @Override
    public EMQXCheckpoint snapshotState(long checkpointId) throws Exception {
        // TODO Auto-generated method stub
        LOG.debug("snapshot: {}", checkpointId);
        return new EMQXCheckpoint();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // TODO Auto-generated method
        LOG.debug("checkpoint complete: {}", checkpointId);
        SplitEnumerator.super.notifyCheckpointComplete(checkpointId);
    }
}
