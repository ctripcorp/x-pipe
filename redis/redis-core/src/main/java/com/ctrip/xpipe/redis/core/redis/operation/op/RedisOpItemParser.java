package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class RedisOpItemParser {

    private static final Logger logger = LoggerFactory.getLogger(RedisOpItemParser.class);

    private RedisOpItemParser() {
    }

    public static RedisOpItem parse(RedisOpParser redisOpParser, Object[] payload) {
        RedisOpItem redisOpItem = new RedisOpItem();
        try {
             RedisOp redisOp = redisOpParser.parse(payload);
            redisOpItem.setRedisOpType(redisOp.getOpType());
            redisOpItem.setGtid(redisOp.getOpGtid());
            redisOpItem.setDbId(redisOp.getDbId());
            if (redisOp instanceof RedisMultiKeyOp) {
                RedisMultiKeyOp redisMultiKeyOp = (RedisMultiKeyOp) redisOp;
                List<RedisKey> keys = redisMultiKeyOp.getKeys();
                redisOpItem.setRedisKeyList(keys);
            } else if (redisOp instanceof RedisMultiSubKeyOp) {
                RedisMultiSubKeyOp redisMultiSubKeyOp = (RedisMultiSubKeyOp) redisOp;
                RedisKey key = redisMultiSubKeyOp.getKey();
                List<RedisKey> subKeys = redisMultiSubKeyOp.getAllSubKeys();
                redisOpItem.setRedisKey(key);
                redisOpItem.setRedisKeyList(subKeys);
            } else if (redisOp instanceof RedisSingleKeyOp) {
                RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
                redisOpItem.setRedisKey(redisSingleKeyOp.getKey());
            }
        } catch (Throwable th) {
            logger.warn("[parse] payload {}, error {}", payload, th.getMessage());
        }
        return redisOpItem;
    }
}
