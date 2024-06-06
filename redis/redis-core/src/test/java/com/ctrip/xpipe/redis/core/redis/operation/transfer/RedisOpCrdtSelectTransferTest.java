package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 20:25
 */
public class RedisOpCrdtSelectTransferTest {

    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtSelectTransfer redisOpCrdtMSetTransfer = new RedisOpCrdtSelectTransfer();
        // // "CRDT.SELECT" "5" "0" - > "SELECT" "0"
        byte[][] args = new byte[][]{"CRDT.SELECT".getBytes(), "5".getBytes(), "0".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtMSetTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SELECT, args);
        Assert.assertEquals(RedisOpType.SELECT, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 2);
        Assert.assertEquals(new String(result[0]), "SELECT");
        Assert.assertEquals(new String(result[1]), "0");
    }
}
