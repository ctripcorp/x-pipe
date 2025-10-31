package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiSubKeyOp;

import java.util.List;

/**
 * @author TB
 * <p>
 * 2025/10/10 17:35
 */
public class RedisMultiSubKeyOpGtidWrapper extends AbstractRedisOpGtidWrapper implements RedisMultiSubKeyOp {
    private RedisMultiSubKeyOp innerRedisMultiSubKeyOp;

    public RedisMultiSubKeyOpGtidWrapper(byte[][] rawGtidArgs, String gtid, String dbid,RedisMultiSubKeyOp innerRedisMultiSubKeyOp) {
        super(rawGtidArgs, gtid, dbid,innerRedisMultiSubKeyOp);
        this.innerRedisMultiSubKeyOp = innerRedisMultiSubKeyOp;
    }

    @Override
    public RedisKey getKey() {
        return this.innerRedisMultiSubKeyOp.getKey();
    }

    @Override
    public List<RedisKey> getAllSubKeys() {
        return this.innerRedisMultiSubKeyOp.getAllSubKeys();
    }
}
