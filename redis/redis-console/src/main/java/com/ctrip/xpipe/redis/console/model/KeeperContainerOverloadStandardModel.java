package com.ctrip.xpipe.redis.console.model;

public class KeeperContainerOverloadStandardModel {

    private long peerDataOverload;

    private long flowOverload;

    public long getPeerDataOverload() {
        return peerDataOverload;
    }

    public KeeperContainerOverloadStandardModel setPeerDataOverload(long peerDataOverload) {
        this.peerDataOverload = peerDataOverload;
        return this;
    }

    public long getFlowOverload() {
        return flowOverload;
    }

    public KeeperContainerOverloadStandardModel setFlowOverload(long flowOverload) {
        this.flowOverload = flowOverload;
        return this;
    }

    @Override
    public String toString() {
        return "KeeperContainerOverloadStandardModel{" +
                "peerDataOverload=" + peerDataOverload +
                ", flowOverload=" + flowOverload +
                '}';
    }
}
