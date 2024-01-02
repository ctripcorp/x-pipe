package com.ctrip.xpipe.redis.console.keeper.entity;

public class IPPairData {
    private long inputFlow;
    private long peerData;
    private int number;

    public IPPairData() {
    }

    public IPPairData(long inputFlow, long peerData, int number) {
        this.inputFlow = inputFlow;
        this.peerData = peerData;
        this.number = number;
    }

    public IPPairData addData(long inputFlow, long peerData) {
        this.inputFlow += inputFlow;
        this.peerData += peerData;
        this.number++;
        return this;
    }

    public IPPairData subData(long inputFlow, long peerData) {
        this.inputFlow -= inputFlow;
        this.peerData -= peerData;
        this.number--;
        return this;
    }


    public long getInputFlow() {
        return inputFlow;
    }

    public void setInputFlow(long inputFlow) {
        this.inputFlow = inputFlow;
    }

    public long getPeerData() {
        return peerData;
    }

    public void setPeerData(long peerData) {
        this.peerData = peerData;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }
}
