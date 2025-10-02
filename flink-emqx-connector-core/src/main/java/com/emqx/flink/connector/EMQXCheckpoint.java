package com.emqx.flink.connector;

import java.io.Serializable;

public class EMQXCheckpoint implements Serializable {
    private long checkpointId;

    EMQXCheckpoint(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    @Override
    public String toString() {
        return String.format("EMQXCheckpoint(%d)", checkpointId);
    }
}
