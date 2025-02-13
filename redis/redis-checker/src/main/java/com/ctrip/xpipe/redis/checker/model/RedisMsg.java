package com.ctrip.xpipe.redis.checker.model;

public class RedisMsg {

    private long inPutFlow;

    private long usedMemory;

    private long offset;

    public RedisMsg() {
    }

    public RedisMsg(long inPutFlow, long usedMemory, long offset) {
        this.inPutFlow = inPutFlow;
        this.usedMemory = usedMemory;
        this.offset = offset;
    }

    public long getInPutFlow() {
        return inPutFlow;
    }

    public void setInPutFlow(long inPutFlow) {
        this.inPutFlow = inPutFlow;
    }

    public long getUsedMemory() {
        return usedMemory;
    }

    public void setUsedMemory(long usedMemory) {
        this.usedMemory = usedMemory;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
