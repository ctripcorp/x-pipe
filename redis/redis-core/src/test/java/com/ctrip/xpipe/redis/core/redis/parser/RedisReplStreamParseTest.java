package com.ctrip.xpipe.redis.core.redis.parser;

import com.ctrip.xpipe.payload.InOutPayloadFactory;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2022/2/22
 */
public class RedisReplStreamParseTest extends AbstractRedisOpParserTest {

    private String redisReplStream = "*5\r\n" +
            "$4\r\n" +
            "GTID\r\n" +
            "$42\r\n" +
            "930832173326c5b1d4a97d059ae43c8164c51ee8:1\r\n" +
            "$3\r\n" +
            "set\r\n" +
            "$2\r\n" +
            "k1\r\n" +
            "$2\r\n" +
            "v1\r\n" +
            "*3\r\n" +
            "$7\r\n" +
            "PUBLISH\r\n" +
            "$18\r\n" +
            "__sentinel__:hello\r\n" +
            "$124\r\n" +
            "10.2.38.97,5002,04c3131a6420fd8a7980ad05da78af67795e78c7,30120,xpipe_function+xpipe_function-shard1+NTGXH,10.2.55.173,6379,0\r\n" +
            "*1\r\n" +
            "$4\r\n" +
            "PING\r\n" +
            "*7\r\n" +
            "$4\r\n" +
            "GTID\r\n" +
            "$42\r\n" +
            "930832173326c5b1d4a97d059ae43c8164c51ee8:2\r\n" +
            "$4\r\n" +
            "mset\r\n" +
            "$2\r\n" +
            "k1\r\n" +
            "$2\r\n" +
            "v1\r\n" +
            "$2\r\n" +
            "k2\r\n" +
            "$2\r\n" +
            "v2\r\n" +
            "*3\r\n" +
            "$7\r\n" +
            "PUBLISH\r\n" +
            "$18\r\n" +
            "__sentinel__:hello\r\n" +
            "$124\r\n" +
            "10.2.38.97,5001,73b8c84c39d3d208d68496c56ce59624a06e6d1b,30120,xpipe_function+xpipe_function-shard1+NTGXH,10.2.55.173,6379,0\r\n" +
            "*5\r\n" +
            "$4\r\n" +
            "GTID\r\n" +
            "$42\r\n" +
            "930832173326c5b1d4a97d059ae43c8164c51ee8:3\r\n" +
            "$3\r\n" +
            "del\r\n" +
            "$2\r\n" +
            "k1\r\n" +
            "$2\r\n" +
            "k2\r\n";
    

    @Test
    public void testReplStreamParse() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(redisReplStream.getBytes());
        List<RedisOp> redisOps = new ArrayList<>();
        while (byteBuf.readableBytes() > 0) {
            ArrayParser arrayParser = new ArrayParser();
            RedisClientProtocol<Object[]> protocol = arrayParser.read(byteBuf);
            RedisOp redisOp = parser.parse(protocol.getPayload());
            redisOps.add(redisOp);
        }

        Assert.assertEquals(RedisOpType.SET, redisOps.get(0).getOpType());
        Assert.assertEquals(RedisOpType.PUBLISH, redisOps.get(1).getOpType());
        Assert.assertEquals(RedisOpType.PING, redisOps.get(2).getOpType());
        Assert.assertEquals(RedisOpType.MSET, redisOps.get(3).getOpType());
        Assert.assertEquals(RedisOpType.PUBLISH, redisOps.get(4).getOpType());
        Assert.assertEquals(RedisOpType.DEL, redisOps.get(5).getOpType());

        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("GTID", "930832173326c5b1d4a97d059ae43c8164c51ee8:1",
                "set", "k1", "v1")), redisOps.get(0).buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("PUBLISH", "__sentinel__:hello",
                "10.2.38.97,5002,04c3131a6420fd8a7980ad05da78af67795e78c7,30120,xpipe_function+xpipe_function-shard1+NTGXH,10.2.55.173,6379,0")),
                redisOps.get(1).buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Collections.singletonList("PING")), redisOps.get(2).buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("GTID", "930832173326c5b1d4a97d059ae43c8164c51ee8:2",
                "mset", "k1", "v1", "k2", "v2")), redisOps.get(3).buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("PUBLISH", "__sentinel__:hello",
                "10.2.38.97,5001,73b8c84c39d3d208d68496c56ce59624a06e6d1b,30120,xpipe_function+xpipe_function-shard1+NTGXH,10.2.55.173,6379,0")),
                redisOps.get(4).buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("GTID", "930832173326c5b1d4a97d059ae43c8164c51ee8:3",
                "del", "k1", "k2")), redisOps.get(5).buildRawOpArgs());
    }

}
