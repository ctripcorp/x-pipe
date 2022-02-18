package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public enum RedisOpType {

    SET(false),
    SETNX(false),
    SETEX(false),
    DEL(true),
    MSET(true),
    SELECT(false),
    PUBLISH(false),
    PING(false),
    MULTI(false),
    EXEC(false),
    UNKNOWN(false);

    private boolean supportMultiKey;

    RedisOpType(boolean multiKey) {
        this.supportMultiKey = multiKey;
    }

    public boolean supportMultiKey() {
        return supportMultiKey;
    }

    public static RedisOpType lookup(String name) {
        if (StringUtil.isEmpty(name)) return UNKNOWN;

        try {
            return valueOf(name.toUpperCase());
        } catch (IllegalArgumentException illegalArgumentException) {
            return UNKNOWN;
        }
    }

}
