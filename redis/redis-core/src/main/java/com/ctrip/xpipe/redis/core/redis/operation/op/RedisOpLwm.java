package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 07:36
 */
public class RedisOpLwm extends AbstractRedisSingleKeyOp implements RedisSingleKeyOp {

    public static final String RAW_CMD = "gtid.lwm";

    public RedisOpLwm(String sid, Long lwm) {
        super(new byte[][] {RAW_CMD.getBytes(), sid.getBytes(), lwm.toString().getBytes()},
                null, null);
    }

    @Override
    public RedisOpType getOpType() {
        return RedisOpType.LWM;
    }
}
