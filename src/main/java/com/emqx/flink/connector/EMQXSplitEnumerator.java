package com.emqx.flink.connector;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.SplitEnumerator;

public class EMQXSplitEnumerator implements SplitEnumerator<EMQXSourceSplit, EMQXCheckpoint> {
    @Override
    public void start() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void addReader(int arg0) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void addSplitsBack(List arg0, int arg1) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void handleSplitRequest(int arg0, @Nullable String arg1) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public EMQXCheckpoint snapshotState(long arg0) throws Exception {
        // TODO Auto-generated method stub
        return new EMQXCheckpoint();
    }
}
