package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 15:27
 */
public class RedisOpCrdtMSetRcTransferTest {
    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtMSetRcTransfer redisOpCrdtMSetRcTransfer = new RedisOpCrdtMSetRcTransfer();
        // "CRDT.MSET_RC" "5" "1718107088036" "hailutest" "3:1:6" "1:1503" "hailutest1" "3:1:7" "1:1503" "hailutest2" "3:1:8" "1:1503" -> "MSET" "hailutest" "6" "hailutest1" "7" "hailutest2" "8"
        byte[][] args = new byte[][]{"CRDT.MSET_RC".getBytes(), "5".getBytes(), "1718107088036".getBytes(), "hailutest".getBytes(), "3:1:6".getBytes(), "1:1503".getBytes(), "hailutest1".getBytes(), "3:1:7".getBytes(), "1:1503".getBytes(), "hailutest2".getBytes(), "3:1:8".getBytes(), "1:1503".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtMSetRcTransfer.transformCrdtRedisOp(RedisOpType.CRDT_MSET_RC, args);
        Assert.assertEquals(RedisOpType.MSET, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 7);
        Assert.assertEquals(new String(result[0]), "MSET");
        Assert.assertEquals(new String(result[1]), "hailutest");
        Assert.assertEquals(new String(result[2]), "6");
        Assert.assertEquals(new String(result[3]), "hailutest1");
        Assert.assertEquals(new String(result[4]), "7");
        Assert.assertEquals(new String(result[5]), "hailutest2");
        Assert.assertEquals(new String(result[6]), "8");

        //"CRDT.MSET_RC" "2" "1719199641151" "hailurc" "3:1:9,2:271279:1:5" "2:271283" "hailurc1" "3:2:10,2:271281:1:1" "2:271283" -> "MSET" "hailurc" "9" "hailurc1" "10"
         args = new byte[][]{"CRDT.MSET_RC".getBytes(), "2".getBytes(), "1719199641151".getBytes(), "hailurc".getBytes(), "3:1:9,2:271279:1:5".getBytes(), "2:271283".getBytes(), "hailurc1".getBytes(), "3:2:10,2:271281:1:1".getBytes(), "2:271283".getBytes()};
         redisOpTypePair = redisOpCrdtMSetRcTransfer.transformCrdtRedisOp(RedisOpType.CRDT_MSET_RC, args);
        Assert.assertEquals(RedisOpType.MSET, redisOpTypePair.getKey());
        result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 5);
        Assert.assertEquals(new String(result[0]), "MSET");
        Assert.assertEquals(new String(result[1]), "hailurc");
        Assert.assertEquals(new String(result[2]), "9");
        Assert.assertEquals(new String(result[3]), "hailurc1");
        Assert.assertEquals(new String(result[4]), "10");

        redisOpCrdtMSetRcTransfer = new RedisOpCrdtMSetRcTransfer();
        // "CRDT.MSET_RC" "5" "1718107088036" "hailutest" "3:1:6" "1:1503"  -> "MSET" "hailutest" "6"
        args = new byte[][]{"CRDT.MSET_RC".getBytes(), "5".getBytes(), "1718107088036".getBytes(), "hailutest".getBytes(), "3:1:6".getBytes(), "1:1503".getBytes()};
        redisOpTypePair = redisOpCrdtMSetRcTransfer.transformCrdtRedisOp(RedisOpType.CRDT_MSET_RC, args);
        Assert.assertEquals(RedisOpType.MSET, redisOpTypePair.getKey());
        result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(new String(result[0]), "MSET");
        Assert.assertEquals(new String(result[1]), "hailutest");
        Assert.assertEquals(new String(result[2]), "6");
    }
}
