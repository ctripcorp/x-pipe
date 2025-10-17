package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public class RedisMultiKeyOpGtidWrapper extends AbstractRedisOpGtidWrapper implements RedisMultiKeyOp {

    private RedisMultiKeyOp innerRedisMultiKeyOp;

    public RedisMultiKeyOpGtidWrapper(byte[][] rawGtidArgs, String gtid, String dbid,RedisMultiKeyOp innerRedisMultiKeyOp) {
        super(rawGtidArgs, gtid, dbid,innerRedisMultiKeyOp);
        this.innerRedisMultiKeyOp = innerRedisMultiKeyOp;
    }

    @Override
    public List<RedisKey> getKeys() {
        return innerRedisMultiKeyOp.getKeys();
    }

    @Override
    public Pair<RedisKey, byte[]> getKeyValue(int idx) {
        return innerRedisMultiKeyOp.getKeyValue(idx);
    }

    @Override
    public List<Pair<RedisKey, byte[]>> getAllKeyValues() {
        return innerRedisMultiKeyOp.getAllKeyValues();
    }

    @Override
    public RedisMultiKeyOp subOp(Set<Integer> needKeys) {
        return new RedisMultiKeyOpGtidWrapper(getRawGtidArgs(), getOpGtid(), getDbId(),innerRedisMultiKeyOp.subOp(needKeys));
    }

}
