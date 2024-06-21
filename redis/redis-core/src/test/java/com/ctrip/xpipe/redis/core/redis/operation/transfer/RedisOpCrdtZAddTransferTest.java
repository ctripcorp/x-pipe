package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 15:27
 */
public class RedisOpCrdtZAddTransferTest {
    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtZAddTransfer redisOpCrdtZAddTransfer = new RedisOpCrdtZAddTransfer();
        // "CRDT.ZADD" "hailusortedset" "5" "1706183768965" "5:28" "hi" "2:70" "world" "2:90" -> "ZADD" "hailusortedset" "70" "hi" "90" "world"
        byte[][] args = new byte[][]{"CRDT.ZADD".getBytes(), "hailusortedset".getBytes(), "5".getBytes(), "1706183768965".getBytes(), "5:28".getBytes(), "hi".getBytes(), "2:70".getBytes(), "world".getBytes(), "2:90".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtZAddTransfer.transformCrdtRedisOp(RedisOpType.CRDT_ZADD, args);
        Assert.assertEquals(RedisOpType.ZADD, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 6);
        Assert.assertEquals(new String(result[0]), "ZADD");
        Assert.assertEquals(new String(result[1]), "hailusortedset");
        Assert.assertEquals(new String(result[2]), "70");
        Assert.assertEquals(new String(result[3]), "hi");
        Assert.assertEquals(new String(result[4]), "90");
        Assert.assertEquals(new String(result[5]), "world");

        redisOpCrdtZAddTransfer = new RedisOpCrdtZAddTransfer();
        // "CRDT.ZADD" "hailusortedset" "5" "1706183768965" "5:28" "hi" "2:70" -> "ZADD" "hailusortedset" "70" "hi"
        args = new byte[][]{"CRDT.ZADD".getBytes(), "hailusortedset".getBytes(), "5".getBytes(), "1706183768965".getBytes(), "5:28".getBytes(), "hi".getBytes(), "2:70".getBytes()};
         redisOpTypePair = redisOpCrdtZAddTransfer.transformCrdtRedisOp(RedisOpType.CRDT_ZADD, args);
        Assert.assertEquals(RedisOpType.ZADD, redisOpTypePair.getKey());
         result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 4);
        Assert.assertEquals(new String(result[0]), "ZADD");
        Assert.assertEquals(new String(result[1]), "hailusortedset");
        Assert.assertEquals(new String(result[2]), "70");
        Assert.assertEquals(new String(result[3]), "hi");
    }
}
