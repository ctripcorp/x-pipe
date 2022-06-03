package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public class RedisOpDel extends AbstractRedisMultiKeyOp implements RedisMultiKeyOp {

    public RedisOpDel(byte[][] rawArgs, List<Pair<RedisKey, byte[]>> redisKvs) {
        super(rawArgs, redisKvs);
    }

    public RedisOpDel(byte[][] rawArgs, List<Pair<RedisKey, byte[]>> redisKvs, String gtid) {
        super(rawArgs, redisKvs, gtid);
    }

    public RedisOpDel(byte[] rawCmdArg, List<Pair<RedisKey, byte[]>> redisKvs) {
        super(rawCmdArg, redisKvs);
    }

    public RedisOpDel(byte[] rawCmdArg, List<Pair<RedisKey, byte[]>> redisKvs, String gtid) {
        super(rawCmdArg, redisKvs, gtid);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.DEL;
    }

    @Override
    public RedisMultiKeyOp subOp(Set<Integer> needKeys) {
        return new RedisOpDel(getRawCmdArg(), subKvs(needKeys), getOpGtid());
    }

    @Override
    protected byte[][] innerBuildRawOpArgs() {
        List<RedisKey> keys = getKeys();
        byte[][] args = new byte[1 + keys.size()][];
        args[0] = getRawCmdArg();

        for (int i = 0; i < keys.size(); i++) {
            args[i + 1] = keys.get(i).get();
        }
        return args;
    }
}
