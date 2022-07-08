package com.ctrip.xpipe.redis.core.redis.rdb;

import com.ctrip.xpipe.redis.core.redis.exception.RdbParseFailException;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/5/30
 */
public class RdbLength {

    private final RdbLenType lenType;

    private final long lenValue;

    public RdbLength(RdbLenType lenType, long lenValue) {
        this.lenType = lenType;
        this.lenValue = lenValue;
    }

    public RdbLenType getLenType() {
        return lenType;
    }

    public int getLenValue() {
        if (lenValue > Integer.MAX_VALUE) {
            throw new RdbParseFailException("can't convert to int: " + lenValue);
        }
        return (int) lenValue;
    }

    public long getLenLongValue() {
        return lenValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RdbLength rdbLength = (RdbLength) o;
        return lenType == rdbLength.lenType &&
                Objects.equals(lenValue, rdbLength.lenValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lenType, lenValue);
    }

    @Override
    public String toString() {
        return lenType + ":" + lenValue;
    }
}
