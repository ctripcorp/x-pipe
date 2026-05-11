package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.redis.core.redis.operation.transfer.*;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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
    HSETEX(false, -4),
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
    CRDT_SET(false, -7, RedisOpCrdtSetTransfer.getInstance()),
    CRDT_MSET(true, -6, RedisOpCrdtMSetTransfer.getInstance()),
    CRDT_DEL_REG(false, -5, RedisOpCrdtDelTransfer.getInstance()),

    //crdt rc
    CRDT_RC(false, 7, RedisOpCrdtRcTransfer.getInstance()),
    CRDT_COUNTER(false, 8, RedisOpCrdtCounterTransfer.getInstance()),
    CRDT_MSET_RC(true, -6, RedisOpCrdtMSetRcTransfer.getInstance()),
    CRDT_DEL_RC(false, -5, RedisOpCrdtDelTransfer.getInstance()),

    //crdt hash
    CRDT_HSET(false, -5, RedisOpCrdtHSetTransfer.getInstance()),
    CRDT_REM_HASH(false, -6, RedisOpCrdtRemHashTransfer.getInstance()),
    CRDT_DEL_HASH(false, 6, RedisOpCrdtDelTransfer.getInstance()),

    //crdt set
    CRDT_SADD(false, -6, RedisOpCrdtSAddTransfer.getInstance()),
    CRDT_SREM(false, -6, RedisOpCrdtSremTransfer.getInstance()),
    CRDT_DEL_SET(false, -5, RedisOpCrdtDelTransfer.getInstance()),

    //crdt sortedSet
    CRDT_ZADD(false, -7, RedisOpCrdtZAddTransfer.getInstance()),
    CRDT_ZREM(false, -6, RedisOpCrdtZRemTransfer.getInstance()),
    CRDT_ZINCRBY(false, -7, RedisOpCrdtZIncrbyTransfer.getInstance()),
    CRDT_DEL_SS(false, 5, RedisOpCrdtDelTransfer.getInstance()),

    //crdt other
    CRDT_SELECT(false, 3, RedisOpCrdtSelectTransfer.getInstance()),
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

    private static final Map<String, RedisOpType> NAME_CACHE = new HashMap<>();
    static {
        for (RedisOpType op : values()) {
            // 存储小写 key，同时将 '.' 替换为 '_' 的版本也存入（与原有 replace 兼容）
            String lower = op.name().toLowerCase(Locale.ROOT);
            NAME_CACHE.put(lower, op);
            // 若名称包含 '_'，则同时存储 '.' 版本，避免 replace 操作
            if (lower.indexOf('_') >= 0) {
                NAME_CACHE.put(lower.replace('_', '.'), op);
            }
        }
    }

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
        if (StringUtil.isEmpty(name)) {
            return UNKNOWN;
        }
        try {
            return valueOf(name.replace('.', '_').toUpperCase());
        } catch (IllegalArgumentException illegalArgumentException) {
            return UNKNOWN;
        }
    }

    public static RedisOpType lookup(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return UNKNOWN;
        }
        // 1. 快速路径：根据首字节和长度匹配常用命令（避免 HashMap 开销和 String 创建）
//        RedisOpType fast = lookupFast(bytes);
//        if (fast != null) {
//            return fast;
//        }
        String s = new String(bytes, StandardCharsets.ISO_8859_1);
        return NAME_CACHE.getOrDefault(s.toLowerCase(Locale.ROOT), UNKNOWN);
    }

    private static RedisOpType lookupFast(byte[] b) {
        int len = b.length;
        if (len <= 1) return null;
        byte c = b[0];
        if(c >= 'A' && c <= 'Z') c += 32;
        switch (c) {
            case 'a': // APPEND
                if (len == 6 && eq(b, 1, 'p','p','e','n','d')) return APPEND;
                break;
            case 'd': // DECRBY, DECR, DEL
                if (len == 3 && eq(b, 1, 'e','l')) return DEL;
                if (len == 4 && eq(b, 1, 'e','c','r')) return DECR;
                if (len == 6 && eq(b, 1, 'e','c','r','b','y')) return DECRBY;
                break;
            case 'e': // EXEC, EXPIRE, EXPIREAT
                if (len == 4 && eq(b, 1, 'x','e','c')) return EXEC;
                if (len == 6 && eq(b, 1, 'x','p','i','r','e')) return EXPIRE;
                if (len == 7 && eq(b, 1, 'x','p','i','r','e','a','t')) return EXPIREAT;
                break;
            case 'g': // GTID_LWM, GETSET, GEOADD, GEORADIUS
                // GTID_LWM 由具体二进制格式决定，此处不进行匹配
                break;
            case 'h': // HDEL, HINCRBY, HINCRBYFLOAT, HMSET, HSET, HSETEX, HSETNX
                if (len == 4) {
                    if (eq(b, 1, 'd','e','l')) return HDEL;
                    if (eq(b, 1, 's','e','t')) return HSET;
                }
                if (len == 5 && eq(b, 1, 'h','m','s','e','t')) return HMSET;
                if (len == 6) {
                    if (eq(b, 1, 'h','s','e','t','e','x')) return HSETEX;
                    if (eq(b, 1, 'h','s','e','t','n','x')) return HSETNX;
                }
                if (len == 7 && eq(b, 1, 'i','n','c','r','b','y')) return HINCRBY;
                if (len == 12 && eq(b, 1, 'i','n','c','r','b','y','f','l','o','a','t')) return HINCRBYFLOAT;
                break;
            case 'i': // INCR, INCRBY, INCRBYFLOAT
                if (len == 4 && eq(b, 1, 'n','c','r')) return INCR;
                if (len == 6 && eq(b, 1, 'n','c','r','b','y')) return INCRBY;
                if (len == 11 && eq(b, 1, 'n','c','r','b','y','f','l','o','a','t')) return INCRBYFLOAT;
                break;
            case 'l': // LINSERT, LPOP, LPUSH, LPUSHX, LREM, LSET, LTRIM
                if (len == 4) {
                    if (eq(b, 1, 'p','o','p')) return LPOP;
                    if (eq(b, 1, 'r','e','m')) return LREM;
                    if (eq(b, 1, 's','e','t')) return LSET;
                }
                if (len == 5) {
                    if (eq(b, 1, 'p','u','s','h')) return LPUSH;
                    if (eq(b, 1, 't','r','i','m')) return LTRIM;
                }
                if (len == 6 && eq(b, 1, 'p','u','s','h','x')) return LPUSHX;
                if (len == 7 && eq(b, 1, 'i','n','s','e','r','t')) return LINSERT;
                break;
            case 'm': // MERGE_END, MERGE_START, MOVE, MSET, MSETNX, MULTI
                if (len == 4 && eq(b, 1, 's','e','t')) return MSET;
                if (len == 5) {
                    if (eq(b, 1, 'o','v','e')) return MOVE;
                    if (eq(b, 1, 'u','l','t','i')) return MULTI;
                }
                if (len == 6 && eq(b, 1, 's','e','t','n','x')) return MSETNX;
                break;
            case 'p': // PERSIST, PEXPIRE, PEXPIREAT, PING, PSETEX, PUBLISH
                if (len == 4 && eq(b, 1, 'i','n','g')) return PING;
                if (len == 6 && eq(b, 1, 's','e','t','e','x')) return PSETEX;
                if (len == 7) {
                    if (eq(b, 1, 'e','x','p','i','r','e')) return PEXPIRE;
                    if (eq(b, 1, 'e','r','s','i','s','t')) return PERSIST;
                }
                if (len == 8 && eq(b, 1, 'e','x','p','i','r','e','a','t')) return PEXPIREAT;
                if (len == 9 && eq(b, 1, 'u','b','l','i','s','h')) return PUBLISH;
                break;
            case 'r': // RPOP, RPUSH, RPUSHX
                if (len == 4 && eq(b, 1, 'p','o','p')) return RPOP;
                if (len == 5 && eq(b, 1, 'p','u','s','h')) return RPUSH;
                if (len == 6 && eq(b, 1, 'p','u','s','h','x')) return RPUSHX;
                break;
            case 's': // SADD, SCRIPT, SELECT, SET, SETBIT, SETEX, SETNX, SETRANGE, SPOP, SREM
                if (len == 3) {
                    if (eq(b, 1, 'e','t')) return SET;
                } else if (len == 4) {
                    if (eq(b, 1, 'a','d','d')) return SADD;
                    if (eq(b, 1, 'p','o','p')) return SPOP;
                    if (eq(b, 1, 'r','e','m')) return SREM;
                } else if (len == 5) {
                    if (eq(b, 1, 'e','t','e','x')) return SETEX;
                    if (eq(b, 1, 'e','t','n','x')) return SETNX;
                } else if (len == 6) {
                    if (eq(b, 1, 'e','t','b','i','t')) return SETBIT;
                    if (eq(b, 1, 'c','r','i','p','t')) return SCRIPT;
                    if (eq(b, 1, 'e','l','e','c','t')) return SELECT;
                } else if (len == 8 && eq(b, 1, 'e','t','r','a','n','g','e')) return SETRANGE;
                break;
            case 'x': // XADD, XCLAIM, XDEL, XGROUP, XSETID
                if (len == 4 && eq(b, 1, 'a','d','d')) return XADD;
                if (len == 5 && eq(b, 1, 'd','e','l')) return XDEL;
                if (len == 6) {
                    if (eq(b, 1, 'c','l','a','i','m')) return XCLAIM;
                    if (eq(b, 1, 's','e','t','i','d')) return XSETID;
                    if (eq(b, 1, 'g','r','o','u','p')) return XGROUP;
                }
                break;
            case 'z': // ZADD, ZINCRBY, ZREM, ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE
                if (len == 4 && eq(b, 1, 'a','d','d')) return ZADD;
                if (len == 4 && eq(b, 1, 'r','e','m')) return ZREM;
                if (len == 8 && eq(b, 1, 'i','n','c','r','b','y')) return ZINCRBY;
                break;
            default:
                break;
        }
        return null;
    }

    private static boolean eq(byte[] b, int start, char... chars) {
        if (b.length < start + chars.length) return false;
        for (int i = 0; i < chars.length; i++) {
            byte b1 = b[start+i];
            if(b1 >= 'A' && b1 <= 'Z') b1 += 32;
            if(b1 != (byte) chars[i]) return false;
        }
        return true;
    }

}
