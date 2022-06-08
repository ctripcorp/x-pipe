package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.utils.StringUtil;

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
    APPEND(false, 3),
    LPOP(false, -2),
    RPOP(false, -2),
    EXPIRE(false, 3),
    EXPIREAT(false, 3),
    PEXPIRE(false, 3),
    PEXPIREAT(false, 3),
    PERSIST(false, 2),
    GEOADD(false, -5),
    GEORADIUS(false, -6),
    HDEL(false, -3),
    HINCRBY(false, 4),
    HINCRBYFLOAT(false, 4),
    HMSET(false, -4),
    HSET(false, -4),
    HSETNX(false, 4),
    LINSERT(false, 5),
    LPUSH(false, -3),
    LPUSHX(false, -3),
    RPUSH(false, -3),
    RPUSHX(false, -3),
    LREM(false, 4),
    LSET(false, 4),
    LTRIM(false, 4),
    MOVE(false, 3),

    // String multi
    DEL(true, -2),
    MSET(true, -3),
    MSETNX(true, -3),

    // other
    SELECT(false, 2),
    PUBLISH(false, 3),
    LWM(false, 3),
    PING(false, -1),
    MULTI(false, 1),
    EXEC(false, 1),
    FLUSHALL(false, -1),
    FLUSHDB(false, -1),
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
