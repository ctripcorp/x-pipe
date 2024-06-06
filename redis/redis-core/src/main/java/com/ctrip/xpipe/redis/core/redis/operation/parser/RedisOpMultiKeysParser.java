package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMultiKVs;
import com.ctrip.xpipe.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ayq
 * <p>
 * 2022/6/8 15:27
 */
public class RedisOpMultiKeysParser extends AbstractRedisOpParser implements RedisOpParser {

    private RedisOpType redisOpType;
    private Integer keyStartIndex;
    private Integer kvNum;

    public RedisOpMultiKeysParser(RedisOpType redisOpType, int keyStartIndex, int kvNum) {
        this.redisOpType = redisOpType;
        this.keyStartIndex = keyStartIndex;
        this.kvNum = kvNum;
    }

    @Override
    public RedisOp parse(byte[][] args) {
        Pair<RedisOpType, byte[][]> pair = redisOpType.transfer(redisOpType, args);
        args = pair.getValue();
        if (0 != (args.length - keyStartIndex) % kvNum) {
            throw new IllegalArgumentException("wrong number of arguments for " + redisOpType.name());
        }
        List<Pair<RedisKey, byte[]>> kvs = new ArrayList<>();

        int i = keyStartIndex;
        while (i < args.length) {
            RedisKey key = new RedisKey(args[i++]);
            kvs.add(kvNum == 1 ? Pair.of(key, null) : Pair.of(key, args[i++]));
        }

        return new RedisOpMultiKVs(pair.getKey(), args, kvs);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
