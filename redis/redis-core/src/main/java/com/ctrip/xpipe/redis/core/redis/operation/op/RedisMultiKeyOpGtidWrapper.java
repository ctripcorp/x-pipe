package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public class RedisMultiKeyOpGtidWrapper<T> extends AbstractRedisOpGtidWrapper implements RedisMultiKeyOp<T> {

    private RedisMultiKeyOp<T> innerRedisMultiKeyOp;

    public RedisMultiKeyOpGtidWrapper(GtidSet gtidSet, RedisMultiKeyOp<T> innerRedisMultiKeyOp) {
        super(gtidSet, innerRedisMultiKeyOp);
        this.innerRedisMultiKeyOp = innerRedisMultiKeyOp;
    }

    @Override
    public List<RedisKey> getKeys() {
        return innerRedisMultiKeyOp.getKeys();
    }

    @Override
    public Pair<RedisKey, T> getKeyValue(int idx) {
        return innerRedisMultiKeyOp.getKeyValue(idx);
    }

    @Override
    public List<Pair<RedisKey, T>> getAllKeyValues() {
        return innerRedisMultiKeyOp.getAllKeyValues();
    }

    @Override
    public RedisMultiKeyOp<T> subOp(Set<Integer> needKeys) {
        return new RedisMultiKeyOpGtidWrapper<>(getOpGtidSet(), innerRedisMultiKeyOp.subOp(needKeys));
    }

}
