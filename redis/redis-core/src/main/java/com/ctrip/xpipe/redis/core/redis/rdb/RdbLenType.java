package com.ctrip.xpipe.redis.core.redis.rdb;

import static com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant.*;

/**
 * @author lishanglin
 * date 2022/5/30
 */
public enum RdbLenType {

    LEN_BIT6(false, (byte) 6),
    LEN_BIT14(false, (byte) 14),
    LEN_BIT32(true, (byte) 32),
    LEN_BIT64(true, (byte) 64),
    LEN_ENC(false, (byte) 6);

    private final boolean skipLenTypeByte;

    private final byte bitsAfterTypeBits;

    RdbLenType(boolean skipLenTypeByte, byte bitsAfterTypeBits) {
        this.skipLenTypeByte = skipLenTypeByte;
        this.bitsAfterTypeBits = bitsAfterTypeBits;
    }

    public boolean needSkipLenTypeByte() {
        return skipLenTypeByte;
    }

    public byte getBitsAfterTypeBits() {
        return bitsAfterTypeBits;
    }

    public static RdbLenType parse(short type) {
        int lenType = (type&0xC0) >>> 6;
        switch (lenType) {
            case REDIS_RDB_LEN_6BITLEN:
                return LEN_BIT6;
            case REDIS_RDB_LEN_14BITLEN:
                return LEN_BIT14;
            case REDIS_RDB_LEN_LONG:
                if (REDIS_RDB_LEN_32BITLEN == type) {
                    return LEN_BIT32;
                } else if (REDIS_RDB_LEN_64BITLEN == type) {
                    return LEN_BIT64;
                } else {
                    return null;
                }
            case REDIS_RDB_LEN_ENCVAL:
                return LEN_ENC;
            default:
                return null;
        }
    }

}
