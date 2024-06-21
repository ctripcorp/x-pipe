package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 15:27
 */
public class RedisOpCrdtSAddTransferTest {
    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtSAddTransfer redisOpCrdtSAddTransfer = new RedisOpCrdtSAddTransfer();
        // "CRDT.SADD" "hailuset" "5" "1706183385248" "5:27" "hello" "world" -> "SADD" "hailuset" "hello" "world"
        byte[][] args = new byte[][]{"CRDT.SADD".getBytes(), "hailuset".getBytes(), "5".getBytes(), "1706183385248".getBytes(), "5:27".getBytes(), "hello".getBytes(), "world".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtSAddTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SADD, args);
        Assert.assertEquals(RedisOpType.SADD, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 4);
        Assert.assertEquals(new String(result[0]), "SADD");
        Assert.assertEquals(new String(result[1]), "hailuset");
        Assert.assertEquals(new String(result[2]), "hello");
        Assert.assertEquals(new String(result[3]), "world");

        redisOpCrdtSAddTransfer = new RedisOpCrdtSAddTransfer();
        // "CRDT.SADD" "hailuset" "5" "1706183385248" "5:27" "hello" -> "SADD" "hailuset" "hello"
         args = new byte[][]{"CRDT.SADD".getBytes(), "hailuset".getBytes(), "5".getBytes(), "1706183385248".getBytes(), "5:27".getBytes(), "hello".getBytes()};
         redisOpTypePair = redisOpCrdtSAddTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SADD, args);
        Assert.assertEquals(RedisOpType.SADD, redisOpTypePair.getKey());
       result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(new String(result[0]), "SADD");
        Assert.assertEquals(new String(result[1]), "hailuset");
        Assert.assertEquals(new String(result[2]), "hello");
    }
}
