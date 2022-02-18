package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public class RedisOpDel extends AbstractRedisMultiKeyOp<Void> implements RedisMultiKeyOp<Void> {

    public RedisOpDel(List<String> rawArgs, List<Pair<RedisKey, Void>> redisKvs) {
        super(rawArgs, redisKvs);
    }

    public RedisOpDel(List<String> rawArgs, List<Pair<RedisKey, Void>> redisKvs, GtidSet gtidSet) {
        super(rawArgs, redisKvs, gtidSet);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.DEL;
    }

    @Override
    public RedisMultiKeyOp<Void> subOp(Set<Integer> needKeys) {
        return new RedisOpDel(null, subKvs(needKeys), getOpGtidSet());
    }

    @Override
    protected List<String> innerBuildRawOpArgs() {
        return Collections.emptyList();
    }
}
