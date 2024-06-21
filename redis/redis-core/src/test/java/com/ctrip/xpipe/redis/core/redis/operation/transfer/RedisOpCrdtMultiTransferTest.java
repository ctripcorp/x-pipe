package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 20:25
 */
public class RedisOpCrdtMultiTransferTest {

    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtMultiTransfer redisOpCrdtMultiTransfer = new RedisOpCrdtMultiTransfer();
        // "CRDT.MULTI" "2" - > "MULTI"
        byte[][] args = new byte[][]{"CRDT.MULTI".getBytes(), "2".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtMultiTransfer.transformCrdtRedisOp(RedisOpType.CRDT_MULTI, args);
        Assert.assertEquals(RedisOpType.MULTI, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 1);
        Assert.assertEquals(new String(result[0]), "MULTI");
    }
}
