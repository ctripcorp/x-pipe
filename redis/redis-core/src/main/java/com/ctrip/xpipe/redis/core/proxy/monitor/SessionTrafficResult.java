package com.ctrip.xpipe.redis.core.proxy.monitor;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

import java.util.Objects;

public class SessionTrafficResult {

    private final long timestamp;

    private final long inputBytes;

    private final long outputBytes;

    private final long inputRates;

    private final long outputRates;


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
}
