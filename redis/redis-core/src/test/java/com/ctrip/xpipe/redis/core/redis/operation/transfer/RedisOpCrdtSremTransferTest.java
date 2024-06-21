package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 15:27
 */
public class RedisOpCrdtSremTransferTest {
    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtSremTransfer redisOpCrdtSremTransfer = new RedisOpCrdtSremTransfer();
        // "CRDT.Srem" "hailuset" "5" "1706183434918" "5:28" "hello" "world" -> "SREM" "hailuset" "hello" "world"
        byte[][] args = new byte[][]{"CRDT.Srem".getBytes(), "hailuset".getBytes(), "5".getBytes(), "1706183434918".getBytes(), "5:28".getBytes(), "hello".getBytes(), "world".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtSremTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SREM, args);
        Assert.assertEquals(RedisOpType.SREM, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 4);
        Assert.assertEquals(new String(result[0]), "SREM");
        Assert.assertEquals(new String(result[1]), "hailuset");
        Assert.assertEquals(new String(result[2]), "hello");
        Assert.assertEquals(new String(result[3]), "world");

        redisOpCrdtSremTransfer = new RedisOpCrdtSremTransfer();
        // "CRDT.Srem" "hailuset" "5" "1706183434918" "5:28" "hello" -> "SREM" "hailuset" "hello"
         args = new byte[][]{"CRDT.Srem".getBytes(), "hailuset".getBytes(), "5".getBytes(), "1706183434918".getBytes(), "5:28".getBytes(), "hello".getBytes()};
         redisOpTypePair = redisOpCrdtSremTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SREM, args);
        Assert.assertEquals(RedisOpType.SREM, redisOpTypePair.getKey());
         result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(new String(result[0]), "SREM");
        Assert.assertEquals(new String(result[1]), "hailuset");
        Assert.assertEquals(new String(result[2]), "hello");
    }
}
