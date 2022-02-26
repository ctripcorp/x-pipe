package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

import java.util.ArrayList;
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

    public RedisOpDel(List<String> rawArgs, List<Pair<RedisKey, Void>> redisKvs, String gtid) {
        super(rawArgs, redisKvs, gtid);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.DEL;
    }

    @Override
    public RedisMultiKeyOp<Void> subOp(Set<Integer> needKeys) {
        return new RedisOpDel(null, subKvs(needKeys), getOpGtid());
    }

    @Override
    protected List<String> innerBuildRawOpArgs() {
        List<RedisKey> keys = getKeys();
        List<String> args = new ArrayList<>(1 + keys.size());
        args.add(getOpType().name());
        for (RedisKey key: keys) {
            args.add(key.get());
        }

        return args;
    }
}
