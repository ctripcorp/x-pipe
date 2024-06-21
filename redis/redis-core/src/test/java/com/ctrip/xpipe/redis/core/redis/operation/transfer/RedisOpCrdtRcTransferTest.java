package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/11 20:09
 */
public class RedisOpCrdtRcTransferTest {
    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtRcTransfer opCrdtRcTransfer = new RedisOpCrdtRcTransfer();
        //"CRDT.rc" "hailu1945" "5" "1706183161935" "5:13" "3:1:1" "-1" -> "SET" "hailu1945" "1"
        byte[][] args = new byte[][]{"CRDT.rc".getBytes(), "hailu1945".getBytes(), "5".getBytes(), "1706183161935".getBytes(), "5:13".getBytes(), "3:1:1".getBytes(), "-1".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = opCrdtRcTransfer.transformCrdtRedisOp(RedisOpType.CRDT_RC, args);
        Assert.assertEquals(RedisOpType.SET, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(new String(result[0]), "SET");
        Assert.assertEquals(new String(result[1]), "hailu1945");
        Assert.assertEquals(new String(result[2]), "1");

        // // "CRDT.rc" "hailu1945" "5" "1706183161935" "5:13" "3:1:1" "-1" -> "PSETEX" "hailu1945" "5000" "1"
        long expire = System.currentTimeMillis() + 3000;
        args = new byte[][]{"CRDT.rc".getBytes(), "hailu1945".getBytes(), "5".getBytes(), "1706183161935".getBytes(), "5:13".getBytes(), "3:1:1".getBytes(), Long.toString(expire).getBytes()};
        redisOpTypePair = opCrdtRcTransfer.transformCrdtRedisOp(RedisOpType.CRDT_RC, args);
        Assert.assertEquals(RedisOpType.PSETEX, redisOpTypePair.getKey());
        result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 4);
        Assert.assertEquals(new String(result[0]), "PSETEX");
        Assert.assertEquals(new String(result[1]), "hailu1945");
        Assert.assertEquals(new String(result[3]), "1");

        // // "CRDT.rc" "hailu1945" "5" "1706183161935" "5:13" "3:1:1" "-1" -> "PSETEX" "hailu1945" "5000" "1"
        expire = System.currentTimeMillis() - 3000;
        args = new byte[][]{"CRDT.rc".getBytes(), "hailu1945".getBytes(), "5".getBytes(), "1706183161935".getBytes(), "5:13".getBytes(), "3:1:1".getBytes(), Long.toString(expire).getBytes()};
        redisOpTypePair = opCrdtRcTransfer.transformCrdtRedisOp(RedisOpType.CRDT_RC, args);
        Assert.assertEquals(RedisOpType.UNKNOWN, redisOpTypePair.getKey());
    }
}
