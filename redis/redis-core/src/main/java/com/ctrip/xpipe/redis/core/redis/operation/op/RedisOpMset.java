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
public class RedisOpMset extends AbstractRedisMultiKeyOp implements RedisMultiKeyOp {

    public RedisOpMset(byte[][] rawArgs, List<Pair<RedisKey, byte[]>> redisKvs) {
        super(rawArgs, redisKvs);
    }

    public RedisOpMset(byte[][] rawArgs, List<Pair<RedisKey, byte[]>> redisKvs, String gtid) {
        super(rawArgs, redisKvs, gtid);
    }

    public RedisOpMset(byte[] rawCmdArg, List<Pair<RedisKey, byte[]>> redisKvs) {
        super(rawCmdArg, redisKvs);
    }

    public RedisOpMset(byte[] rawCmdArg, List<Pair<RedisKey, byte[]>> redisKvs, String gtid) {
        super(rawCmdArg, redisKvs, gtid);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.MSET;
    }

    @Override
    public RedisMultiKeyOp subOp(Set<Integer> needKeys) {
        return new RedisOpMset(getRawCmdArg(), subKvs(needKeys), getOpGtid());
    }

    @Override
    protected byte[][] innerBuildRawOpArgs() {
        List<Pair<RedisKey, byte[]>> kvs = getAllKeyValues();
        byte[][] args = new byte[1 + 2 * kvs.size()][];
        args[0] = getRawCmdArg();

        for (int i = 0; i < kvs.size(); i++) {
            args[2 * i  + 1] = kvs.get(i).getKey().get();
            args[2 * i + 2] = kvs.get(i).getValue();
        }
        return args;
    }
}
