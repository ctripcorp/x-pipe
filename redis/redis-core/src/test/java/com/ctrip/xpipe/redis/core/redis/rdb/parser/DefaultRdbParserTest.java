package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbDataBytes.*;

/**
 * @author lishanglin
 * date 2022/6/5
 */
public class DefaultRdbParserTest extends AbstractTest implements RdbParseListener {

    private DefaultRdbParser parser;

    private DefaultRdbParseContext context;

    private List<RedisOp> redisOps;

    @Before
    public void setupDefaultRdbParserTest() {
        context = new DefaultRdbParseContext();
        parser = new DefaultRdbParser(context);
        parser.registerListener(this);
        parser.needFinishNotify(true);
        redisOps = new ArrayList<>();
    }

    @Test
    public void testParseKv() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(kvOnlyRdbBytes);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("ccd78ed08986e8843b87203a728b205d4573444b:1-3," +
                "bb0d9be7816adfe9fa29d0747b0024b2a3400b98:1," +
                "0bbb52aa066feb5ae6bb4e30bd56aa4f8a2016ac:1-4", context.getAux("gtid"));
        Assert.assertEquals(0, context.getDbId());
        Assert.assertEquals(5, redisOps.size());
        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("SET k4 100", redisOps.get(1).toString());
        Assert.assertEquals("SET k2 v2", redisOps.get(2).toString());
        Assert.assertEquals("SET k1 v1", redisOps.get(3).toString());
        Assert.assertEquals("SET k3 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", redisOps.get(4).toString());
    }

    @Test
    public void testParseExpire() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(kvWithExpire);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("SET k2 v2", redisOps.get(1).toString());
        Assert.assertEquals("PEXPIREAT k2 1655461780643", redisOps.get(2).toString());
        Assert.assertEquals("SET k1 v1", redisOps.get(3).toString());
        Assert.assertEquals("PEXPIREAT k1 1655461819085", redisOps.get(4).toString());
    }

    @Test
    public void testParseQuicklist() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(quicklistRdbBytes);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("RPUSH arr k1", redisOps.get(1).toString());
        Assert.assertEquals("RPUSH arr k2", redisOps.get(2).toString());
        Assert.assertEquals("RPUSH arr 10", redisOps.get(3).toString());
        Assert.assertEquals("RPUSH arr 100", redisOps.get(4).toString());
        Assert.assertEquals("RPUSH arr -100", redisOps.get(5).toString());
        Assert.assertEquals("RPUSH arr 65535", redisOps.get(6).toString());
        Assert.assertEquals("RPUSH arr aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", redisOps.get(7).toString());
    }

    @Test
    public void testParseHashtable() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(hashtableRdbBytes);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("HSET h1 k4 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", redisOps.get(1).toString());
        Assert.assertEquals("HSET h1 k2 akl;jsdnaskjndckjslfncieukandakjsndkajsndkasjdnkajebndkajsdn", redisOps.get(2).toString());
        Assert.assertEquals("HSET h1 k1 v1", redisOps.get(3).toString());
        Assert.assertEquals("HSET h1 k3 9999999999999999999999999999999999999999999999999999", redisOps.get(4).toString());
        Assert.assertEquals("HSET h1 k5 qwertyuwadbiausbdkjasbndkjasbnckjsndkjasndkjasbndkjasbndkjasbndkjasnkdjnaskjdnaskjdnkasjdnkasndkjasndkjasnkd", redisOps.get(5).toString());
}

    @Test
    public void testParseHashZiplist() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(ziplistHashRdbBytes);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("HSET h1 k1 v1", redisOps.get(1).toString());
        Assert.assertEquals("HSET h1 k2 10", redisOps.get(2).toString());
        Assert.assertEquals("HSET h1 k3 100", redisOps.get(3).toString());
        Assert.assertEquals("HSET h1 k4 -100", redisOps.get(4).toString());
        Assert.assertEquals("HSET h1 k5 65535", redisOps.get(5).toString());
        Assert.assertEquals("HSET h1 k6 4294967296", redisOps.get(6).toString());
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {
        logger.info("[onRedisOp] {}", redisOp);
        redisOps.add(redisOp);
    }

    @Override
    public void onAux(String key, String value) {
        logger.info("[onAux] {} {}", key, value);
    }

    @Override
    public void onFinish(RdbParser<?> parser) {
        logger.info("[onFinish] {}", parser);
    }
}
