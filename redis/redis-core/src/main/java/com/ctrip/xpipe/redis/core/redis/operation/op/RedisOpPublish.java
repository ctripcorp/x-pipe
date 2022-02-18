package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public class RedisOpPublish extends AbstractRedisSingleKeyOp<String> implements RedisSingleKeyOp<String> {

    public RedisOpPublish(List<String> rawArgs, RedisKey redisKey, String redisValue) {
        super(rawArgs, redisKey, redisValue);
    }

    public RedisOpPublish(List<String> rawArgs, RedisKey redisKey, String redisValue, GtidSet gtidSet) {
        super(rawArgs, redisKey, redisValue, gtidSet);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.PUBLISH;
    }
}
