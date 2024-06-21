package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 16:32
 */
public class RedisOpCrdtSetTransferTest {

    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtSetTransfer redisOpCrdtSetTransfer = new RedisOpCrdtSetTransfer();
        //"CRDT.SET" "hailu2001" "hailu" "5" "1706184079769" "2:177847;5:35" "-1" -> "SET" "hailu2001" "hailu"
        byte[][] args = new byte[][]{"CRDT.SET".getBytes(), "hailu2001".getBytes(), "hailu".getBytes(), "5".getBytes(), "1706184079769".getBytes(), "2:177847;5:35".getBytes(), "-1".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtSetTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SET, args);
        Assert.assertEquals(RedisOpType.SET, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(new String(result[0]), "SET");
        Assert.assertEquals(new String(result[1]), "hailu2001");
        Assert.assertEquals(new String(result[2]), "hailu");

        // "CRDT.SET" "hailu2001" "hailu" "5" "1706184079769" "2:177847;5:35" "1706184109769" -> "PSETEX" "hailu2001" "5000" "hailu"
        long expire = System.currentTimeMillis() + 3000;
        args = new byte[][]{"CRDT.SET".getBytes(), "hailu2001".getBytes(), "hailu".getBytes(), "5".getBytes(), "1706184079769".getBytes(), "2:177847;5:35".getBytes(), Long.toString(expire).getBytes()};
        redisOpTypePair = redisOpCrdtSetTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SET, args);
        Assert.assertEquals(RedisOpType.PSETEX, redisOpTypePair.getKey());
        result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 4);
        Assert.assertEquals(new String(result[0]), "PSETEX");
        Assert.assertEquals(new String(result[1]), "hailu2001");
        Assert.assertEquals(new String(result[3]), "hailu");

        // "CRDT.SET" "hailu2001" "hailu" "5" "1706184079769" "2:177847;5:35" "1706184069769" -> "PSETEX" "hailu2001" "5000" "hailu"
        expire = System.currentTimeMillis() - 3000;
        args = new byte[][]{"CRDT.SET".getBytes(), "hailu2001".getBytes(), "hailu".getBytes(), "5".getBytes(), "1706184079769".getBytes(), "2:177847;5:35".getBytes(), Long.toString(expire).getBytes()};
        redisOpTypePair = redisOpCrdtSetTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SET, args);
        Assert.assertEquals(RedisOpType.UNKNOWN, redisOpTypePair.getKey());
    }
}
