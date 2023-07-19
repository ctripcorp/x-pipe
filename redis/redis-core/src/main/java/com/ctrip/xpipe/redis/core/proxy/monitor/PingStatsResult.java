package com.ctrip.xpipe.redis.core.proxy.monitor;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;

import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class PingStatsResult {

    private long start;
    private long end;
    private HostPort direct;
    private HostPort real;

    public PingStatsResult() {
    }

    public PingStatsResult(long start, long end, HostPort direct, HostPort real) {
        this.start = start;
        this.end = end;
        this.direct = direct;
        this.real = real;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public HostPort getDirect() {
        return direct;
    }

    public HostPort getReal() {
        return real;
    }

    public PingStatsResult setStart(long start) {
        this.start = start;
        return this;
    }

    public PingStatsResult setEnd(long end) {
        this.end = end;
        return this;
    }

    public PingStatsResult setDirect(HostPort direct) {
        this.direct = direct;
        return this;
    }

    public PingStatsResult setReal(HostPort real) {
        this.real = real;
        return this;
    }

    public Object toArrayObject() {
        Object[] result = new Object[4];
        result[0] = start;
        result[1] = end;
        result[2] = direct.toString();
        result[3] = real.toString();
        return result;
    }

    public static PingStatsResult parse(Object object) {
        if(!object.getClass().isArray()) {
            throw new XpipeRuntimeException("Should parse from an array");
        }
        Object[] input = (Object[]) object;
        long start = (Long) input[0];
        long end = (Long) input[1];
        HostPort direct = HostPort.fromString(input[2].toString());
        HostPort real = HostPort.fromString(input[3].toString());
        return new PingStatsResult(start, end, direct, real);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PingStatsResult that = (PingStatsResult) o;
        return start == that.start &&
                end == that.end &&
                Objects.equals(direct, that.direct) &&
                Objects.equals(real, that.real);
    }

    @Override
    public int hashCode() {

        return Objects.hash(start, end, direct, real);
    }

    @Override
    public String toString() {
        return "PingStatsResult{" +
                "start=" + start +
                ", end=" + end +
                ", direct=" + direct.toString() +
                ", real=" + real.toString() +
                '}';
    }
}
