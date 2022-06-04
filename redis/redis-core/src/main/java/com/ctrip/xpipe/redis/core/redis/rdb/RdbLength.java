package com.ctrip.xpipe.redis.core.redis.rdb;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/5/30
 */
public class RdbLength {

    private final RdbLenType lenType;

//    private final UnsignedLong unsignedLong;
    // too complex to support unsignedLong
    private final int lenValue;

    public RdbLength(RdbLenType lenType, int lenValue) {
        this.lenType = lenType;
        this.lenValue = lenValue;
    }

    public RdbLenType getLenType() {
        return lenType;
    }

    public int getLenValue() {
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
