package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author Slight
 * <p>
 * Oct 11, 2022 16:06
 */
public class RedisOpMergeEnd extends AbstractRedisSingleKeyOp implements RedisSingleKeyOp {

    public static final String RAW_CMD = "ctrip.merge_end";

    public RedisOpMergeEnd(String gtidSet) {
        super(new byte[][]{RAW_CMD.getBytes(), gtidSet.getBytes()}, null, null);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.MERGE_END;
    }
}
