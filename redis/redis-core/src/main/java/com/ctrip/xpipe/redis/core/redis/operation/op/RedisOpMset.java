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
public class RedisOpMset extends AbstractRedisMultiKeyOp<String> implements RedisMultiKeyOp<String> {

    public RedisOpMset(List<String> rawArgs, List<Pair<RedisKey, String>> redisKvs) {
        super(rawArgs, redisKvs);
    }

    public RedisOpMset(List<String> rawArgs, List<Pair<RedisKey, String>> redisKvs, GtidSet gtidSet) {
        super(rawArgs, redisKvs, gtidSet);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.MSET;
    }

    @Override
    public RedisMultiKeyOp<String> subOp(Set<Integer> needKeys) {
        return new RedisOpMset(null, subKvs(needKeys), getOpGtidSet());
    }

    @Override
    protected List<String> innerBuildRawOpArgs() {
        return Collections.emptyList();
    }
}
