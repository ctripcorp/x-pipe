package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 15:27
 */
public class RedisOpCrdtHDelTransferTest {
    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtHDelTransfer redisOpCrdtHDelTransfer = new RedisOpCrdtHDelTransfer();
        // "CRDT.REM_HASH" "hailu1937" "1" "1718161692326" "1:1513;2:1500" "hailutest1" "hailutest2" -> "HDEL" "hailu1937" "hailutest1" "hailutest2"
        byte[][] args = new byte[][]{"CRDT.REM_HASH".getBytes(), "hailu1937".getBytes(), "1".getBytes(), "1718161692326".getBytes(), "1:1513;2:1500".getBytes(), "hailutest1".getBytes(), "hailutest2".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtHDelTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SREM, args);
        Assert.assertEquals(RedisOpType.HDEL, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 4);
        Assert.assertEquals(new String(result[0]), "HDEL");
        Assert.assertEquals(new String(result[1]), "hailu1937");
        Assert.assertEquals(new String(result[2]), "hailutest1");
        Assert.assertEquals(new String(result[3]), "hailutest2");

         redisOpCrdtHDelTransfer = new RedisOpCrdtHDelTransfer();
        // "CRDT.REM_HASH" "hailu1937" "1" "1718161692326" "1:1513;2:1500" "hailutest1"  -> "HDEL" "hailu1937" "hailutest1"
        args = new byte[][]{"CRDT.REM_HASH".getBytes(), "hailu1937".getBytes(), "1".getBytes(), "1718161692326".getBytes(), "1:1513;2:1500".getBytes(), "hailutest1".getBytes()};
        redisOpTypePair = redisOpCrdtHDelTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SREM, args);
        Assert.assertEquals(RedisOpType.HDEL, redisOpTypePair.getKey());
        result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(new String(result[0]), "HDEL");
        Assert.assertEquals(new String(result[1]), "hailu1937");
        Assert.assertEquals(new String(result[2]), "hailutest1");
    }
}
