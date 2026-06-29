package com.ctrip.xpipe.redis.keeper.store.gtid.index;

/**
 * @author TB
 * @date 2026/6/28 17:41
 */
public enum IndexEntryType {
    GTID((byte) 0),
    ZONE((byte) 1);

    private final byte code;

    IndexEntryType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static IndexEntryType fromCode(byte code) {
        for (IndexEntryType type : values()) {
            if (type.code == code) return type;
        }
        throw new IllegalArgumentException("Unknown EntryType code: " + code);
    }
}
