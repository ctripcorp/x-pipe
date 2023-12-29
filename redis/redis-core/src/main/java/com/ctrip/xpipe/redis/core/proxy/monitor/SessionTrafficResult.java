package com.ctrip.xpipe.redis.core.proxy.monitor;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

import java.io.Serializable;
import java.util.Objects;

public class SessionTrafficResult implements Serializable {

    private long timestamp;

    private long inputBytes;

    private long outputBytes;

    private long inputRates;

    private long outputRates;

    public SessionTrafficResult() {
    }

    public SessionTrafficResult(long timestamp, long inputBytes, long outputBytes, long inputRates, long outputRates) {
        this.timestamp = timestamp;
        this.inputBytes = inputBytes;
        this.outputBytes = outputBytes;
        this.inputRates = inputRates;
        this.outputRates = outputRates;
    }

    public Object[] toArray() {
        Object[] objects = new Object[5];
        objects[0] = timestamp;
        objects[1] = inputBytes;
        objects[2] = outputBytes;
        objects[3] = inputRates;
        objects[4] = outputRates;
        return objects;
    }

    public static SessionTrafficResult parseFromArray(Object[] objects) {
        if(!(objects[0] instanceof Long)
                || !(objects[1] instanceof Long)
                || !(objects[2] instanceof Long)) {
            throw new XpipeRuntimeException("first element of SocketStatsResult should be timestamp");
        }
        long timestamp = (long) objects[0];
        long inputBytes = (long) objects[1];
        long outputBytes = (long) objects[2];
        long inputRates = (long) objects[3];
        long outputRates = (long) objects[4];
        return new SessionTrafficResult(timestamp, inputBytes, outputBytes, inputRates, outputRates);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getInputBytes() {
        return inputBytes;
    }

    public long getOutputBytes() {
        return outputBytes;
    }

    public long getInputRates() {
        return inputRates;
    }

    public long getOutputRates() {
        return outputRates;
    }

    public SessionTrafficResult setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public SessionTrafficResult setInputBytes(long inputBytes) {
        this.inputBytes = inputBytes;
        return this;
    }

    public SessionTrafficResult setOutputBytes(long outputBytes) {
        this.outputBytes = outputBytes;
        return this;
    }

    public SessionTrafficResult setInputRates(long inputRates) {
        this.inputRates = inputRates;
        return this;
    }

    public SessionTrafficResult setOutputRates(long outputRates) {
        this.outputRates = outputRates;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SessionTrafficResult that = (SessionTrafficResult) o;
        return timestamp == that.timestamp &&
                inputBytes == that.inputBytes &&
                outputBytes == that.outputBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, inputBytes, outputBytes);
    }

    @Override
    protected SessionTrafficResult clone() {
        SessionTrafficResult clone = new SessionTrafficResult();
        clone.timestamp = this.timestamp;
        clone.inputBytes = this.inputBytes;
        clone.outputBytes = this.outputBytes;
        clone.inputRates = this.inputRates;
        clone.outputRates = this.outputRates;
        return clone;
    }
}
