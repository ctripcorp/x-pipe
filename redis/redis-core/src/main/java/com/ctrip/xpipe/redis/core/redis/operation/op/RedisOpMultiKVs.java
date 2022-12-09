package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpMultiKeysEnum;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @author ayq
 * <p>
 * 2022/6/8 15:44
 */
public class RedisOpMultiKVs extends AbstractRedisMultiKeyOp implements RedisMultiKeyOp {

    private RedisOpType redisOpType;

    public RedisOpMultiKVs(RedisOpType redisOpType, byte[][] rawArgs, List<Pair<RedisKey, byte[]>> redisKvs) {
        super(rawArgs, redisKvs);
        this.redisOpType = redisOpType;
    }

    public RedisOpMultiKVs(RedisOpType redisOpType, byte[][] rawArgs, List<Pair<RedisKey, byte[]>> redisKvs, String gtid) {
        super(rawArgs, redisKvs, gtid);
        this.redisOpType = redisOpType;
    }

    public RedisOpMultiKVs(RedisOpType redisOpType, byte[] rawCmdArg, List<Pair<RedisKey, byte[]>> redisKvs) {
        super(rawCmdArg, redisKvs);
        this.redisOpType = redisOpType;
    }

    public RedisOpMultiKVs(RedisOpType redisOpType, byte[] rawCmdArg, List<Pair<RedisKey, byte[]>> redisKvs, String gtid) {
        super(rawCmdArg, redisKvs, gtid);
        this.redisOpType = redisOpType;
    }

    @Override
    public RedisOpType getOpType() {
        return redisOpType;
    }

    @Override
    public RedisMultiKeyOp subOp(Set<Integer> needKeys) {
        return new RedisOpMultiKVs(redisOpType, getRawCmdArg(), subKvs(needKeys), getOpGtid());
    }

    @Override
    protected byte[][] innerBuildRawOpArgs() {
        List<Pair<RedisKey, byte[]>> kvs = getAllKeyValues();
        int length = 1 + RedisOpMultiKeysEnum.findByRedisOpType(redisOpType).getKvNum() * kvs.size();
        byte[][] args = new byte[length][];
        args[0] = getRawCmdArg();

        int i = 1;
        for (Pair<RedisKey, byte[]> kv : kvs) {
            args[i++] = kv.getKey().get();
            if (kv.getValue() != null) {
                args[i++] = kv.getValue();
            }
        }
        return args;
    }
}
