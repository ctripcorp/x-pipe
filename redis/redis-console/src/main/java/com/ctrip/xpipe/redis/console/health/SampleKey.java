package com.ctrip.xpipe.redis.console.health;

import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Aug 21, 2018
 */
public class SampleKey {

    long startNano;
    long expirePeriodMilli;

    public SampleKey(long startNano, long expirePeriodMilli) {
        this.startNano = startNano;
        this.expirePeriodMilli = expirePeriodMilli;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleKey sampleKey = (SampleKey) o;
        return startNano == sampleKey.startNano &&
                expirePeriodMilli == sampleKey.expirePeriodMilli;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startNano, expirePeriodMilli);
    }
}
