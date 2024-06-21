package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 15:27
 */
public class RedisOpCrdtMSetTransferTest {
    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtMSetTransfer opCrdtMSetTransfer = new RedisOpCrdtMSetTransfer();
        // "CRDT.MSET" "5" "1706184154287" "hailu2002" "hailu" "2:177847;5:38" "hailu2003" "hailu" "2:177847;5:38" "hailu2004" "hailu" "2:177847;5:38" - > "MSET" "hailu2002" "hailu" "hailu2003" "hailu" "hailu2004" "hailu"
        byte[][] args = new byte[][]{"CRDT.MSET".getBytes(), "5".getBytes(), "1706184154287".getBytes(), "hailu2002".getBytes(), "hailu".getBytes(), "2:177847;5:38".getBytes(), "hailu2003".getBytes(), "hailu".getBytes(), "2:177847;5:38".getBytes(), "hailu2004".getBytes(), "hailu".getBytes(), "2:177847;5:38".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = opCrdtMSetTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SET, args);
        Assert.assertEquals(RedisOpType.MSET, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 7);
        Assert.assertEquals(new String(result[0]), "MSET");
        Assert.assertEquals(new String(result[1]), "hailu2002");
        Assert.assertEquals(new String(result[2]), "hailu");
        Assert.assertEquals(new String(result[3]), "hailu2003");
        Assert.assertEquals(new String(result[4]), "hailu");
        Assert.assertEquals(new String(result[5]), "hailu2004");
        Assert.assertEquals(new String(result[6]), "hailu");

        opCrdtMSetTransfer = new RedisOpCrdtMSetTransfer();
        // "CRDT.MSET" "5" "1706184154287" "hailu2002" "hailu" "2:177847;5:38"  - > "MSET" "hailu2002" "hailu"
         args = new byte[][]{"CRDT.MSET".getBytes(), "5".getBytes(), "1706184154287".getBytes(), "hailu2002".getBytes(), "hailu".getBytes(), "2:177847;5:38".getBytes()};
         redisOpTypePair = opCrdtMSetTransfer.transformCrdtRedisOp(RedisOpType.CRDT_SET, args);
        Assert.assertEquals(RedisOpType.MSET, redisOpTypePair.getKey());
         result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(new String(result[0]), "MSET");
        Assert.assertEquals(new String(result[1]), "hailu2002");
        Assert.assertEquals(new String(result[2]), "hailu");
    }
}
