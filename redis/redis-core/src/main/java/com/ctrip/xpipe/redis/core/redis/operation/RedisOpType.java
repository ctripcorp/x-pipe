package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.utils.StringUtil;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public enum RedisOpType {

    // String single
    SET(false, -3),
    SETNX(false, 3),
    SETEX(false, 4),
    PSETEX(false, 4),
    INCR(false, 2),
    DECR(false, 2),
    INCRBY(false, 3),
    DECRBY(false, 3),

    // String multi
    DEL(true, -2),
    MSET(true, -3),

    // other
    SELECT(false, 2),
    PUBLISH(false, 3),
    PING(false, -1),
    MULTI(false, 1),
    EXEC(false, 1),
    UNKNOWN(false, -1);

    // Support multi key or not
    private boolean supportMultiKey;

    // Number of arguments, it is possible to use -N to say >= N
    private int arity;

    RedisOpType(boolean multiKey, int arity) {
        this.supportMultiKey = multiKey;
        this.arity = arity;
    }

    public boolean supportMultiKey() {
        return supportMultiKey;
    }

    public int getArity() {
        return arity;
    }

    public boolean checkArgcNotStrictly(Object[] args) {
        return args.length >= Math.abs(arity);
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
