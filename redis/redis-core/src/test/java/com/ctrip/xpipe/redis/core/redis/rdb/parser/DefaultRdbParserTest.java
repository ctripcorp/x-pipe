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

    @Test
    public void testParseIntset() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(intsetRdbBytes);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("SADD set -3", redisOps.get(1).toString());
        Assert.assertEquals("SADD set -2", redisOps.get(2).toString());
        Assert.assertEquals("SADD set -1", redisOps.get(3).toString());
        Assert.assertEquals("SADD set 1", redisOps.get(4).toString());
        Assert.assertEquals("SADD set 2", redisOps.get(5).toString());
        Assert.assertEquals("SADD set 3", redisOps.get(6).toString());
    }

    @Test
    public void testParseSet() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(setRdbBytes);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("SADD set aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", redisOps.get(1).toString());
        Assert.assertEquals("SADD set -99999999999999", redisOps.get(2).toString());
        Assert.assertEquals("SADD set 13927438904093012", redisOps.get(3).toString());
        Assert.assertEquals("SADD set v1", redisOps.get(4).toString());
    }

    @Test
    public void testParseZiplistZSet() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(ziplistZSetRdbBytes);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("ZADD zset -1.5 v4", redisOps.get(1).toString());
        Assert.assertEquals("ZADD zset -1 v3", redisOps.get(2).toString());
        Assert.assertEquals("ZADD zset 0 v5", redisOps.get(3).toString());
        Assert.assertEquals("ZADD zset 1 v1", redisOps.get(4).toString());
        Assert.assertEquals("ZADD zset 1.5 v2", redisOps.get(5).toString());
    }

    @Test
    public void testParseZSet2() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(zset2RdbBytes);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("ZADD zset2 -1.123123213123123 v5", redisOps.get(1).toString());
        Assert.assertEquals("ZADD zset2 -1 v4", redisOps.get(2).toString());
        Assert.assertEquals("ZADD zset2 0 v3", redisOps.get(3).toString());
        Assert.assertEquals("ZADD zset2 1.4783274983274983 v1", redisOps.get(4).toString());
        Assert.assertEquals("ZADD zset2 2.5 v2", redisOps.get(5).toString());
    }

    @Test
    public void testParseListpackStreamOnlyMsg() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(listpackStreamOnlyMsg);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("XADD stream 1655886901649-0 k1 v1 k2 v2", redisOps.get(1).toString());
        Assert.assertEquals("XDEL stream 1655886913542-0", redisOps.get(2).toString());
        Assert.assertEquals("XADD stream 1655886968769-0 k3 65535 k4 -4096 k5 999999999999999", redisOps.get(3).toString());
        Assert.assertEquals("XADD stream 1655886991081-0 k6 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", redisOps.get(4).toString());
        Assert.assertEquals("XSETID stream 1655886991081-0", redisOps.get(5).toString());
    }

    @Test
    public void testParseListpackStream() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(listpackStream);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("XADD stream 1655886901649-0 k1 v1 k2 v2", redisOps.get(1).toString());
        Assert.assertEquals("XDEL stream 1655886913542-0", redisOps.get(2).toString());
        Assert.assertEquals("XADD stream 1655886968769-0 k3 65535 k4 -4096 k5 999999999999999", redisOps.get(3).toString());
        Assert.assertEquals("XADD stream 1655886991081-0 k6 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", redisOps.get(4).toString());
        Assert.assertEquals("XSETID stream 1655886991081-0", redisOps.get(5).toString());
        Assert.assertEquals("XGROUP CREATE stream cgroup1 1655886991081-0 MKSTREAM", redisOps.get(6).toString());
        Assert.assertEquals("XGROUP SETID stream cgroup1 1655886991081-0", redisOps.get(7).toString());
        Assert.assertEquals("XGROUP CREATECONSUMER stream cgroup1 consumer1", redisOps.get(8).toString());
        Assert.assertEquals("XCLAIM stream cgroup1 consumer1 0 1655886901649-0 TIME 1656039104089 RETRYCOUNT 1 FORCE JUSTID", redisOps.get(9).toString());
        Assert.assertEquals("XGROUP CREATECONSUMER stream cgroup1 consumer2", redisOps.get(10).toString());
        Assert.assertEquals("XCLAIM stream cgroup1 consumer2 0 1655886991081-0 TIME 1656039112201 RETRYCOUNT 1 FORCE JUSTID", redisOps.get(11).toString());
        Assert.assertEquals("XGROUP CREATE stream cgroup2 1655886991081-0 MKSTREAM", redisOps.get(12).toString());
        Assert.assertEquals("XGROUP SETID stream cgroup2 1655886991081-0", redisOps.get(13).toString());
        Assert.assertEquals("XGROUP CREATECONSUMER stream cgroup2 consumer", redisOps.get(14).toString());
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

    @Override
    public void onAuxFinish() {
        logger.info("[onAuxFinish]");
    }
}
