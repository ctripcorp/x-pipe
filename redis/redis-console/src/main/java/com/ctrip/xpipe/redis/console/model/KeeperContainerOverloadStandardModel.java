package com.ctrip.xpipe.redis.console.model;

public class KeeperContainerOverloadStandardModel {

    private int peerDataOverload;

    private int flowOverload;

    public int getPeerDataOverload() {
        return peerDataOverload;
    }

    public KeeperContainerOverloadStandardModel setPeerDataOverload(int peerDataOverload) {
        this.peerDataOverload = peerDataOverload;
        return this;
    }

    public int getFlowOverload() {
        return flowOverload;
    }

    public KeeperContainerOverloadStandardModel setFlowOverload(int flowOverload) {
        this.flowOverload = flowOverload;
        return this;
    }
}
