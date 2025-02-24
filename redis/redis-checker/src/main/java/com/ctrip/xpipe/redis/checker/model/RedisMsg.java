package com.ctrip.xpipe.redis.checker.model;

import java.util.Objects;

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

    public void addRedisMsg(RedisMsg redisMsg) {
        this.inPutFlow += redisMsg.getInPutFlow();
        this.usedMemory += redisMsg.getUsedMemory();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RedisMsg)) return false;
        RedisMsg redisMsg = (RedisMsg) o;
        return getInPutFlow() == redisMsg.getInPutFlow() && getUsedMemory() == redisMsg.getUsedMemory() && getOffset() == redisMsg.getOffset();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getInPutFlow(), getUsedMemory(), getOffset());
    }

    @Override
    public String toString() {
        return "RedisMsg{" +
                "inPutFlow=" + inPutFlow +
                ", usedMemory=" + usedMemory +
                ", offset=" + offset +
                '}';
    }
}
