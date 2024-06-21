package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 15:27
 */
public class RedisOpCrdtHSetTransferTest {
    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtHSetTransfer redisOpCrdtHSetTransfer = new RedisOpCrdtHSetTransfer();
        // "CRDT.HSET" "hailu1937" "5" "1706182703266" "2:177847;5:7" "4" "hailutest1" "hailu" "hailutest2" "hailu" -> "hset" "hailu1937" "hailutest1" "hailu" "hailutest2" "hailu"
        byte[][] args = new byte[][]{"CRDT.HSET".getBytes(), "hailu1937".getBytes(), "5".getBytes(), "1706182703266".getBytes(), "2:177847;5:7".getBytes(), "4".getBytes(), "hailutest1".getBytes(), "hailu".getBytes(), "hailutest2".getBytes(), "hailu".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtHSetTransfer.transformCrdtRedisOp(RedisOpType.CRDT_HSET, args);
        Assert.assertEquals(RedisOpType.HSET, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 6);
        Assert.assertEquals(new String(result[0]), "HSET");
        Assert.assertEquals(new String(result[1]), "hailu1937");
        Assert.assertEquals(new String(result[2]), "hailutest1");
        Assert.assertEquals(new String(result[3]), "hailu");
        Assert.assertEquals(new String(result[4]), "hailutest2");
        Assert.assertEquals(new String(result[5]), "hailu");

        redisOpCrdtHSetTransfer = new RedisOpCrdtHSetTransfer();
        // "CRDT.HSET" "hailu1937" "5" "1706182703266" "2:177847;5:7" "4" "hailutest1" "hailu" -> "hset" "hailu1937" "hailutest1" "hailu"
        args = new byte[][]{"CRDT.HSET".getBytes(), "hailu1937".getBytes(), "5".getBytes(), "1706182703266".getBytes(), "2:177847;5:7".getBytes(), "4".getBytes(), "hailutest1".getBytes(), "hailu".getBytes()};
         redisOpTypePair = redisOpCrdtHSetTransfer.transformCrdtRedisOp(RedisOpType.CRDT_HSET, args);
        Assert.assertEquals(RedisOpType.HSET, redisOpTypePair.getKey());
         result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 4);
        Assert.assertEquals(new String(result[0]), "HSET");
        Assert.assertEquals(new String(result[1]), "hailu1937");
        Assert.assertEquals(new String(result[2]), "hailutest1");
        Assert.assertEquals(new String(result[3]), "hailu");
    }
}
