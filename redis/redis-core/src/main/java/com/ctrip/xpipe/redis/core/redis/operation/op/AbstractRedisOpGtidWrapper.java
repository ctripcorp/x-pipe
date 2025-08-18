package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public abstract class AbstractRedisOpGtidWrapper extends AbstractRedisOp implements RedisOp {

    private byte[][] rawGtidArgs;

    private String gtid;

    private RedisOp innerRedisOp;

    public AbstractRedisOpGtidWrapper(byte[][] rawGtidArgs, String gtid, RedisOp innerRedisOp) {
        this.rawGtidArgs = rawGtidArgs;
        this.gtid = gtid;
        this.innerRedisOp = innerRedisOp;

        this.estimateSize(rawGtidArgs, innerRedisOp.estimatedSize());
    }

    @Override
    public RedisOpType getOpType() {
        return innerRedisOp.getOpType();
    }

    @Override
    public String getOpGtid() {
        return gtid;
    }

    @Override
    public void clearGtid() {
        this.rawGtidArgs = new byte[0][];
        this.gtid = null;
    }

    @Override
    public Long getTimestamp() {
        return innerRedisOp.getTimestamp();
    }

    @Override
    public String getGid() {
        return innerRedisOp.getGid();
    }

    protected byte[][] getRawGtidArgs() {
        return rawGtidArgs;
    }

    @Override
    public byte[][] buildRawOpArgs() {
        byte[][] innerArgs = innerRedisOp.buildRawOpArgs();
        byte[][] wholeArgs = new byte[rawGtidArgs.length + innerArgs.length][];
        System.arraycopy(rawGtidArgs, 0, wholeArgs, 0, rawGtidArgs.length);
        System.arraycopy(innerArgs, 0, wholeArgs, rawGtidArgs.length, innerArgs.length);
        return wholeArgs;
    }
}
