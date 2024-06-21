package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.redis.core.redis.operation.transfer.*;
import com.ctrip.xpipe.tuple.Pair;
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
    GETSET(false, 3),
    INCR(false, 2),
    DECR(false, 2),
    INCRBY(false, 3),
    INCRBYFLOAT(false, 3),
    DECRBY(false, 3),
    APPEND(false, 3),
    SETRANGE(false, 4),

    // List single
    LPOP(false, -2),
    RPOP(false, -2),
    LINSERT(false, 5),
    LPUSH(false, -3),
    LPUSHX(false, -3),
    RPUSH(false, -3),
    RPUSHX(false, -3),
    LREM(false, 4),
    LSET(false, 4),
    LTRIM(false, 4),

    // Hash single
    HDEL(false, -3),
    HINCRBY(false, 4),
    HINCRBYFLOAT(false, 4),
    HMSET(false, -4),
    HSET(false, -4),
    HSETNX(false, 4),

    // Set single
    SADD(false, -3),
    SPOP(false, -2),
    SREM(false, -3),

    // ZSet single
    ZADD(false, -4),
    ZINCRBY(false, 4),
    ZREM(false, -3),
    ZREMRANGEBYLEX(false, 4),
    ZREMRANGEBYRANK(false, 4),
    ZREMRANGEBYSCORE(false, 4),

    // Stream single
    XADD(false, -5),
    XDEL(false, -3),
    XSETID(false, 3),
    XGROUP(false, -2),
    XCLAIM(false, -6),

    // TTL single
    EXPIRE(false, 3),
    EXPIREAT(false, 3),
    PEXPIRE(false, 3),
    PEXPIREAT(false, 3),
    PERSIST(false, 2),

    // Geo single
    GEOADD(false, -5),
    GEORADIUS(false, -6),

    // Bit single
    SETBIT(false, 4),

    // String multi
    DEL(true, -2),
    UNLINK(true, -2),
    MSET(true, -3),
    MSETNX(true, -3),

    // ctrip
    GTID_LWM(false, 3, false),
    CTRIP_MERGE_START(false, -1, true),
    CTRIP_MERGE_END(false, -2, true),

    // other
    SELECT(false, 2),
    PUBLISH(false, 3),
    LWM(false, 3),
    MERGE_START(false, 1),
    MERGE_END(false, 2),
    PING(false, -1),
    MULTI(false, 1),
    EXEC(false, 1),
    SCRIPT(false, -2),
    MOVE(false, 3),
    UNKNOWN(false, -1, true),

    //crdt register
    CRDT_SET(false, 3, RedisOpCrdtSetTransfer.getInstance()),
    CRDT_MSET(true, -6, RedisOpCrdtMSetTransfer.getInstance()),
    CRDT_DEL_REG(false, 5, RedisOpCrdtDelTransfer.getInstance()),

    //crdt rc
    CRDT_RC(false, 7, RedisOpCrdtRcTransfer.getInstance()),
    CRDT_COUNTER(false, 8, RedisOpCrdtCounterTransfer.getInstance()),
    CRDT_MSET_RC(true, -6, RedisOpCrdtMSetRcTransfer.getInstance()),
    CRDT_DEL_RC(false, 6, RedisOpCrdtDelTransfer.getInstance()),

    //crdt hash
    CRDT_HSET(false, -8, RedisOpCrdtHSetTransfer.getInstance()),
    CRDT_REM_HASH(false, -7, RedisOpCrdtRemHashTransfer.getInstance()),
    CRDT_HDEL(false, -6, RedisOpCrdtHDelTransfer.getInstance()),
    CRDT_DEL_HASH(false, 6, RedisOpCrdtDelTransfer.getInstance()),

    //crdt set
    CRDT_SADD(false, -6, RedisOpCrdtSAddTransfer.getInstance()),
    CRDT_SREM(false, -6, RedisOpCrdtSremTransfer.getInstance()),
    CRDT_DEL_SET(false, 6, RedisOpCrdtDelTransfer.getInstance()),

    //crdt sortedSet
    CRDT_ZADD(false, -7, RedisOpCrdtZAddTransfer.getInstance()),
    CRDT_ZREM(false, -6, RedisOpCrdtZRemTransfer.getInstance()),
    CRDT_ZINCRBY(false, 8, RedisOpCrdtZIncrbyTransfer.getInstance()),
    CRDT_DEL_SS(false, 5, RedisOpCrdtDelTransfer.getInstance()),

    //crdt other
    CRDT_SELECT(false, 2, RedisOpCrdtSelectTransfer.getInstance()),
    CRDT_MULTI(false, 2, RedisOpCrdtMultiTransfer.getInstance()),
    CRDT_EXEC(false, 2, RedisOpCrdtExecTransfer.getInstance()),
    CRDT_EXPIRE(false, 6, RedisOpCrdtExpireTransfer.getInstance()),
    CRDT_PERSIST(false, 4, RedisOpCrdtPersistTransfer.getInstance()),
    CRDT_OVC(false, 2, true),
    CRDT_PUBLISH(false, 3, true);

    // Support multi key or not
    private boolean supportMultiKey;

    // Number of arguments, it is possible to use -N to say >= N
    private int arity;

    private boolean swallow;

    private RedisOpCrdtTransfer transfer;

    RedisOpType(boolean multiKey, int arity) {
        this(multiKey, arity, false);
    }

    RedisOpType(boolean multiKey, int arity, boolean swallow) {
        this(multiKey, arity, swallow, null);
    }

    RedisOpType(boolean multiKey, int arity, RedisOpCrdtTransfer transfer) {
        this(multiKey, arity, false, transfer);
    }

    RedisOpType(boolean multiKey, int arity, boolean swallow, RedisOpCrdtTransfer transfer) {
        this.supportMultiKey = multiKey;
        this.arity = arity;
        this.swallow = swallow;
        this.transfer = transfer;
    }

    public boolean supportMultiKey() {
        return supportMultiKey;
    }

    public int getArity() {
        return arity;
    }

    public boolean isSwallow() {
        return swallow;
    }

    public Pair<RedisOpType, byte[][]> transfer(RedisOpType redisOpType, byte[][] args) {
        if (null == transfer) {
            return Pair.of(redisOpType, args);
        }
        return this.transfer.transformCrdtRedisOp(redisOpType, args);
    }

    public boolean checkArgcNotStrictly(Object[] args) {
        return args.length >= Math.abs(arity);
    }

    public static RedisOpType lookup(String name) {
        if (StringUtil.isEmpty(name)) return UNKNOWN;

        try {
            return valueOf(name.replace('.', '_').toUpperCase());
        } catch (IllegalArgumentException illegalArgumentException) {
            return UNKNOWN;
        }
    }
}
