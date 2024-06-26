package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 15:27
 */
public class RedisOpCrdtZRemTransferTest {
    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtZRemTransfer redisOpCrdtZRemTransfer = new RedisOpCrdtZRemTransfer();
        // "CRDT.Zrem" "hailusortedset" "5" "1706183987080" "5:34" "3:5:hello" "3:5:world" -> "ZREM" "hailusortedset" "hello" "world"
        byte[][] args = new byte[][]{"CRDT.Zrem".getBytes(), "hailusortedset".getBytes(), "5".getBytes(), "1706183987080".getBytes(), "5:34".getBytes(), "3:5:hello".getBytes(), "3:5:world".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtZRemTransfer.transformCrdtRedisOp(RedisOpType.CRDT_ZREM, args);
        Assert.assertEquals(RedisOpType.ZREM, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 4);
        Assert.assertEquals(new String(result[0]), "ZREM");
        Assert.assertEquals(new String(result[1]), "hailusortedset");
        Assert.assertEquals(new String(result[2]), "hello");
        Assert.assertEquals(new String(result[3]), "world");

        redisOpCrdtZRemTransfer = new RedisOpCrdtZRemTransfer();
        // "CRDT.Zrem" "hailusortedset" "5" "1706183987080" "5:34" "3:5:hello" -> "ZREM" "hailusortedset" "hello"
        args = new byte[][]{"CRDT.Zrem".getBytes(), "hailusortedset".getBytes(), "5".getBytes(), "1706183987080".getBytes(), "5:34".getBytes(), "3:5:hello".getBytes()};
        redisOpTypePair = redisOpCrdtZRemTransfer.transformCrdtRedisOp(RedisOpType.CRDT_ZREM, args);
        Assert.assertEquals(RedisOpType.ZREM, redisOpTypePair.getKey());
         result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(new String(result[0]), "ZREM");
        Assert.assertEquals(new String(result[1]), "hailusortedset");
        Assert.assertEquals(new String(result[2]), "hello");

        redisOpCrdtZRemTransfer = new RedisOpCrdtZRemTransfer();
        // "CRDT.Zrem" "hailusortedset" "5" "1706183987080" "3:5:hello,2:271271:2:20" -> "ZREM" "hailusortedset" "hello"
        args = new byte[][]{"CRDT.Zrem".getBytes(), "hailusortedset".getBytes(), "5".getBytes(), "1706183987080".getBytes(), "5:34".getBytes(), "3:5:hello,2:271271:2:20".getBytes()};
        redisOpTypePair = redisOpCrdtZRemTransfer.transformCrdtRedisOp(RedisOpType.CRDT_ZREM, args);
        Assert.assertEquals(RedisOpType.ZREM, redisOpTypePair.getKey());
         result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(new String(result[0]), "ZREM");
        Assert.assertEquals(new String(result[1]), "hailusortedset");
        Assert.assertEquals(new String(result[2]), "hello");
    }
}
