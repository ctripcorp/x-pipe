package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 15:27
 */
public class RedisOpCrdtZIncrbyTransferTest {
    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtZIncrbyTransfer redisOpCrdtZIncrbyTransfer = new RedisOpCrdtZIncrbyTransfer();
        // "CRDT.Zincrby" "hailusortedset" "1" "1717742949954" "1:6" "hello" "2:20" "80" -> "ZADD" "hailusortedset" "80" "hello"
        byte[][] args = new byte[][]{"CRDT.Zincrby".getBytes(), "hailusortedset".getBytes(), "1".getBytes(), "1717742949954".getBytes(), "1:6".getBytes(), "hello".getBytes(), "2:20".getBytes(), "80".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtZIncrbyTransfer.transformCrdtRedisOp(RedisOpType.CRDT_ZINCRBY, args);
        Assert.assertEquals(RedisOpType.ZADD, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 4);
        Assert.assertEquals(new String(result[0]), "ZADD");
        Assert.assertEquals(new String(result[1]), "hailusortedset");
        Assert.assertEquals(new String(result[2]), "80");
        Assert.assertEquals(new String(result[3]), "hello");
    }
}
