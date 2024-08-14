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
import java.util.Map;

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
    public void testParseList() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(listRdbBytes);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("RPUSH test_0_7743 aaa0_0_530", redisOps.get(1).toString());
        Assert.assertEquals("RPUSH test_0_7743 aaa0_0_454", redisOps.get(2).toString());
        Assert.assertEquals("RPUSH test_0_7743 aaa1_1_39", redisOps.get(3).toString());
        Assert.assertEquals("RPUSH test_0_7743 aaa1_1_244", redisOps.get(4).toString());

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

    @Test
    public void testParseCrdtRegister() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(crdtRegister);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }
        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("SET test35 test35", redisOps.get(1).toString());
        Assert.assertEquals("SET test2 test2", redisOps.get(2).toString());
        Assert.assertEquals("SET test0 test0", redisOps.get(3).toString());
        Assert.assertEquals("SET test43 test43", redisOps.get(4).toString());
        Assert.assertEquals("SET test37 test37", redisOps.get(5).toString());
        Assert.assertEquals("SET test34 test34", redisOps.get(6).toString());
    }

    @Test
    public void testParseCrdtRc() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(crdtRc);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }
        Assert.assertEquals("SET test311 44", redisOps.get(0).toString());
        Assert.assertEquals("SET test78 112", redisOps.get(1).toString());
        Assert.assertEquals("SET test170 75", redisOps.get(2).toString());
        Assert.assertEquals("SET test206 140", redisOps.get(3).toString());
        Assert.assertEquals("SET test230 133", redisOps.get(4).toString());
        Assert.assertEquals("SET test303 150", redisOps.get(5).toString());
        Assert.assertEquals("SET test15 99", redisOps.get(6).toString());
        Assert.assertEquals("SET test34 96", redisOps.get(7).toString());
    }
    @Test
    public void testParseCrdtHash() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(crdtHash);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }
        Assert.assertEquals("HSET testHash141 141key141 141value141", redisOps.get(0).toString());
        Assert.assertEquals("HSET testHash141 0key3 0value4", redisOps.get(1).toString());
        Assert.assertEquals("HSET testHash141 4key73 4value1", redisOps.get(2).toString());
        Assert.assertEquals("HSET testHash141 3key52 3value13", redisOps.get(3).toString());
        Assert.assertEquals("HSET testHash141 1key94 1value11", redisOps.get(4).toString());
        Assert.assertEquals("HSET testHash141 2key63 2value5", redisOps.get(5).toString());
        Assert.assertEquals("HSET testHash414 2key56 2value17", redisOps.get(6).toString());
        Assert.assertEquals("HSET testHash414 414key414 414value414", redisOps.get(7).toString());
        Assert.assertEquals("HSET testHash414 0key83 0value46", redisOps.get(8).toString());
        Assert.assertEquals("HSET testHash414 1key74 1value66", redisOps.get(9).toString());
        Assert.assertEquals("HSET testHash414 3key46 3value4", redisOps.get(10).toString());
        Assert.assertEquals("HSET testHash414 4key61 4value87", redisOps.get(11).toString());
    }
    @Test
    public void testParseCrdtSet() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(crdtSet);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }
        Assert.assertEquals("SADD testSet210 0key71", redisOps.get(0).toString());
        Assert.assertEquals("SADD testSet210 3key86", redisOps.get(1).toString());
        Assert.assertEquals("SADD testSet210 2key53", redisOps.get(2).toString());
        Assert.assertEquals("SADD testSet210 1key24", redisOps.get(3).toString());
        Assert.assertEquals("SADD testSet210 4key95", redisOps.get(4).toString());
        Assert.assertEquals("SADD testSet299 2key96", redisOps.get(5).toString());
        Assert.assertEquals("SADD testSet299 1key0", redisOps.get(6).toString());
        Assert.assertEquals("SADD testSet299 3key74", redisOps.get(7).toString());
        Assert.assertEquals("SADD testSet299 0key22", redisOps.get(8).toString());
        Assert.assertEquals("SADD testSet299 4key14", redisOps.get(9).toString());
    }

    @Test
    public void testParseCrdtSortedSet() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(crdtSortedSet);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }
        Assert.assertEquals("ZADD testSS400 -1789212410 0key41", redisOps.get(0).toString());
        Assert.assertEquals("ZADD testSS400 0.30632634869200115 2key83", redisOps.get(1).toString());
        Assert.assertEquals("ZADD testSS400 430319757 1key55", redisOps.get(2).toString());
        Assert.assertEquals("ZADD testSS400 1129121678 2key40", redisOps.get(3).toString());
        Assert.assertEquals("ZADD testSS400 0.9097036375974098 1key62", redisOps.get(4).toString());
        Assert.assertEquals("ZADD testSS400 0.7535691384206059 3key56", redisOps.get(5).toString());
        Assert.assertEquals("ZADD testSS400 15090449 4key72", redisOps.get(6).toString());
        Assert.assertEquals("ZADD testSS400 0.13377163121944524 4key58", redisOps.get(7).toString());
        Assert.assertEquals("ZADD testSS400 -850281224 3key95", redisOps.get(8).toString());
        Assert.assertEquals("ZADD testSS400 0.16882870577120113 0key23", redisOps.get(9).toString());
        Assert.assertEquals("ZADD testSS329 0.2222225990616864 2key34", redisOps.get(10).toString());
        Assert.assertEquals("ZADD testSS329 0.7775372794341464 0key41", redisOps.get(11).toString());
        Assert.assertEquals("ZADD testSS329 -855389314 3key58", redisOps.get(12).toString());
        Assert.assertEquals("ZADD testSS329 0.014929504313835218 1key52", redisOps.get(13).toString());
        Assert.assertEquals("ZADD testSS329 -616587597 2key25", redisOps.get(14).toString());
        Assert.assertEquals("ZADD testSS329 0.02372256176843568 3key44", redisOps.get(15).toString());
        Assert.assertEquals("ZADD testSS329 1664534196 4key86", redisOps.get(16).toString());
        Assert.assertEquals("ZADD testSS329 -1302475107 0key77", redisOps.get(17).toString());
        Assert.assertEquals("ZADD testSS329 1156336817 1key24", redisOps.get(18).toString());
        Assert.assertEquals("ZADD testSS329 0.2700072245105105 4key89", redisOps.get(19).toString());
    }

    @Test
    public void testParseBitmap() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(rorBitmap);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("SET mykey ", redisOps.get(1).toString());

        byte[][] bitmapKey2 = redisOps.get(2).buildRawOpArgs();
        Assert.assertEquals("bitmap_key2", new String(bitmapKey2[1]));
        Assert.assertEquals(700 / 8 + (700 % 8 == 0 ? 0 : 1),bitmapKey2[2].length);
        byte[][] bitmapKey = redisOps.get(3).buildRawOpArgs();
        Assert.assertEquals("bitmap_key", new String(bitmapKey[1]));
        Assert.assertEquals(700 / 8 + (700 % 8 == 0 ? 0 : 1),bitmapKey[2].length);
        Assert.assertNotEquals(0, bitmapKey[2][87] & (1 << 3)); // getbit bitmap_key 700  ->  1
        Assert.assertNotEquals(0, bitmapKey[2][8] & (1 << 1)); // getbit bitmap_key 70  ->  1
        Assert.assertEquals("SET common_key value", redisOps.get(4).toString());
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
    public void onAuxFinish(Map<String, String> auxMap) {
        logger.info("[onAuxFinish]");
    }
}
