package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author Slight
 * <p>
 * Oct 11, 2022 16:00
 */
public class RedisOpMergeStart extends AbstractRedisSingleKeyOp implements RedisSingleKeyOp {

    public static final String RAW_CMD = "ctrip.merge_start";

    public RedisOpMergeStart() {
        super(new byte[][]{RAW_CMD.getBytes()}, null, null);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.MERGE_START;
    }
}
