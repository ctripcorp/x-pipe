package com.ctrip.xpipe.redis.core.redis.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpLwm;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMergeEnd;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMergeStart;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

/**
 * @author lishanglin
 * date 2022/2/18
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class GeneralRedisOpParserTest extends AbstractRedisOpParserTest {

    @Test
    public void testCtripMergeStartParse() {
        RedisOpMergeStart redisOpMergeStart = new RedisOpMergeStart();
        RedisOp redisOp = parser.parse(redisOpMergeStart.buildRawOpArgs());
        Assert.assertEquals(RedisOpType.CTRIP_MERGE_START, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(redisOpMergeStart.buildRawOpArgs(), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertNull(redisSingleKeyOp.getKey());
        Assert.assertNull(redisSingleKeyOp.getValue());
        Assert.assertTrue(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testCtripMergeEndParse() {
        RedisOpMergeEnd redisOpMergeEnd = new RedisOpMergeEnd("24d9e2513182d156cbd999df5ebedf24e7634140:1-1494763841");
        RedisOp redisOp = parser.parse(redisOpMergeEnd.buildRawOpArgs());
        Assert.assertEquals(RedisOpType.CTRIP_MERGE_END, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(redisOpMergeEnd.buildRawOpArgs(), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertArrayEquals("24d9e2513182d156cbd999df5ebedf24e7634140:1-1494763841".getBytes(), redisSingleKeyOp.getKey().get());
        Assert.assertNull(redisSingleKeyOp.getValue());
        Assert.assertTrue(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testCtripGtidLwmParse() {
        RedisOpLwm redisOpLwm = new RedisOpLwm("24d9e2513182d156cbd999df5ebedf24e7634140", 1494763841L);
        System.out.println(redisOpLwm.buildRawOpArgs());
        RedisOp redisOp = parser.parse(redisOpLwm.buildRawOpArgs());
        Assert.assertEquals(RedisOpType.GTID_LWM, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(redisOpLwm.buildRawOpArgs(), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertArrayEquals("24d9e2513182d156cbd999df5ebedf24e7634140".getBytes(), redisSingleKeyOp.getKey().get());
        Assert.assertArrayEquals("1494763841".getBytes(), redisSingleKeyOp.getValue());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }
    @Test
    public void testUnknowParse() {
        byte[][] rawOpArgs = {"unknow".getBytes(), "unknow_key".getBytes(), "unknow_value".getBytes()};
        RedisOp redisOp = parser.parse(rawOpArgs);
        Assert.assertEquals(RedisOpType.UNKNOWN, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(rawOpArgs, redisOp.buildRawOpArgs());
        byte[][] rawOpArgs1 = {"credis_flushall".getBytes()};
        redisOp = parser.parse(rawOpArgs1);
        Assert.assertEquals(RedisOpType.UNKNOWN, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(rawOpArgs1, redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertNull(redisSingleKeyOp.getKey());
        Assert.assertNull(redisSingleKeyOp.getValue());
        Assert.assertTrue(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("SET", "k1", "v1").toArray());
        Assert.assertEquals(RedisOpType.SET, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("SET", "k1", "v1")), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertArrayEquals("k1".getBytes(), redisSingleKeyOp.getKey().get());
        Assert.assertArrayEquals("v1".getBytes(), redisSingleKeyOp.getValue());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testGtidParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "set", "k1", "v1").toArray());
        Assert.assertEquals(RedisOpType.SET, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("GTID", "a1:10", "0", "set", "k1", "v1")), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertArrayEquals("k1".getBytes(), redisSingleKeyOp.getKey().get());
        Assert.assertArrayEquals("v1".getBytes(), redisSingleKeyOp.getValue());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testMSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("MSET", "k1", "v1", "k2", "v2").toArray());
        Assert.assertEquals(RedisOpType.MSET, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("MSET", "k1", "v1", "k2", "v2")), redisOp.buildRawOpArgs());

        RedisMultiKeyOp redisMultiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(2, redisMultiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("k1"), redisMultiKeyOp.getKeyValue(0).getKey());
        Assert.assertArrayEquals("v1".getBytes(), redisMultiKeyOp.getKeyValue(0).getValue());
        Assert.assertEquals(new RedisKey("k2"), redisMultiKeyOp.getKeyValue(1).getKey());
        Assert.assertArrayEquals("v2".getBytes(), redisMultiKeyOp.getKeyValue(1).getValue());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testGtidMSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "MSET", "k1", "v1", "k2", "v2").toArray());
        Assert.assertEquals(RedisOpType.MSET, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("GTID", "a1:10", "0", "MSET", "k1", "v1", "k2", "v2")), redisOp.buildRawOpArgs());

        RedisMultiKeyOp redisMultiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(2, redisMultiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("k1"), redisMultiKeyOp.getKeyValue(0).getKey());
        Assert.assertArrayEquals("v1".getBytes(), redisMultiKeyOp.getKeyValue(0).getValue());
        Assert.assertEquals(new RedisKey("k2"), redisMultiKeyOp.getKeyValue(1).getKey());
        Assert.assertArrayEquals("v2".getBytes(), redisMultiKeyOp.getKeyValue(1).getValue());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testSelectParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("SELECT", "0").toArray());
        Assert.assertEquals(RedisOpType.SELECT, redisOp.getOpType());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testPingParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("PING", "TEST").toArray());
        Assert.assertEquals(RedisOpType.PING, redisOp.getOpType());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertNull(redisSingleKeyOp.getKey());
        Assert.assertNull(redisSingleKeyOp.getValue());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testNoneExistsCmdParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("EMPTY", "0").toArray());
        Assert.assertEquals(RedisOpType.UNKNOWN, redisOp.getOpType());
        Assert.assertTrue(redisOp.getOpType().isSwallow());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParamShorterParse() {
        parser.parse(Arrays.asList("SET").toArray());
    }

    @Test
    public void testParamLongerParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("SET", "a", "1", "b").toArray());
        Assert.assertEquals(RedisOpType.SET, redisOp.getOpType());
    }

    @Test
    public void testParseAllCmds() {
        List<String> cmdList = Arrays.asList(
                "append",
                "decr",
                "decrby",
                "del",
                "expire",
                "expireat",
                "geoadd",
                "georadius",
                "getset",
                "hdel",
                "hincrby",
                "hincrbyfloat",
                "hmset",
                "hset",
                "hsetex",
                "hsetnx",
                "hexpireat",
                "hpexpireat",
                "hpexpire",
                "hexpire",
                "hgetdel",
                "HPERSIST",
                "HGETEX",
                "incr",
                "incrby",
                "linsert",
                "lpop",
                "BLMPOP",
                "LMPOP",
                "lpush",
                "lpushx",
                "lrem",
                "lset",
                "ltrim",
                "move",
                "mset",
                "msetnx",
                "persist",
                "pexpire",
                "pexpireat",
                "psetex",
                "rpop",
                "rpush",
                "rpushx",
                "sadd",
                "set",
                "setbit",
                "setex",
                "setnx",
                "setrange",
                "spop",
                "srem",
                "unlink",
                "zadd",
                "ZMPOP",
                "BZMPOP",
                "zincrby",
                "zrem",
                "zremrangebylex",
                "zremrangebyrank",
                "zremrangebyscore",
                "incrbyfloat",
                "publish",
                "ping",
                "select",
                "exec",
                "script",
                "multi"
        );
        // 所有使用 FIELDS 语法的命令（需要特殊构造参数）
        Set<String> fieldsCmds = new HashSet<>(Arrays.asList(
                "hsetex", "hgetex", "hgetdel", "hpersist"
        ));

        Set<String> fieldsCmdWithArgs = new HashSet<>(Arrays.asList(
                "hexpire", "hexpireat",
                "hpexpire", "hpexpireat"
        ));

        for (String cmd : cmdList) {
            RedisOpType redisOpType = RedisOpType.lookup(cmd);
            List<String> parserList = new ArrayList<>();
            parserList.add(cmd);   // 第一个元素是命令本身

            if (fieldsCmds.contains(cmd.toLowerCase())) {
                // 构造符合 FIELDS 语法的合法参数序列
                parserList.add("mykey");      // 主键
                parserList.add("FIELDS");
                parserList.add("1");          // numfields = 1
                parserList.add("field1");     // 一个 field
                if ("hsetex".equalsIgnoreCase(cmd)) {
                    parserList.add("value1"); // HSETEX 需要 field-value 对
                }
            } else if(fieldsCmdWithArgs.contains(cmd.toLowerCase())) {
                parserList.add("mykey");
                parserList.add("100");
                parserList.add("FIELDS");
                parserList.add("1");          // numfields = 1
                parserList.add("field1");
            } else {
                // 原有逻辑：根据 arity 填充占位参数
                int arity = redisOpType.getArity();
                int minArgs = Math.abs(arity) - 1; // 减去命令名自身
                for (int i = 0; i < minArgs; i++) {
                    parserList.add("0");
                }
            }

            System.out.println(cmd);
            parser.parse(parserList.toArray());   // 解析不应抛出异常
        }
    }

    @Test
    public void testGTIDHashCmdWithSubKeys(){
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "HMSET","hash1", "k1", "v1", "k2", "v2").toArray());
        Assert.assertEquals(RedisOpType.HMSET, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());


        redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "SADD","set1", "m1","m2").toArray());
        Assert.assertEquals(RedisOpType.SADD, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
    }

    @Test
    public void testGTIDHashExCmdWithSubKeys(){
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "HSETEX","hash1","nx","ex","3600","fields","2", "k1", "v1", "k2", "v2").toArray());
        Assert.assertEquals(RedisOpType.HSETEX, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
    }

    @Test
    public void testGTIDSetCmdWithSubKeys(){
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "SADD","set1", "m1","m2").toArray());
        Assert.assertEquals(RedisOpType.SADD, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
    }

    @Test
    public void testGTIDZSetCmdWithSubKeys(){
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "ZADD","zset1", "1000","zm1","2000","zm2").toArray());
        Assert.assertEquals(RedisOpType.ZADD, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
    }

    @Test
    public void testGTIDZSetAddCmdWithSubKeys(){
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "ZADD","zset1","XX","CH","INCR", "1000","zm1","2000","zm2").toArray());

        Assert.assertEquals(RedisOpType.ZADD, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
    }

    @Test
    public void testGTIDZSetAddCmdWithSubKeysWithXxargs(){
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "ZADD","zset1","xx","ch","INCR", "1000","zm1","2000","zm2").toArray());

        Assert.assertEquals(RedisOpType.ZADD, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
    }



    @Test
    public void testGTIDZGeoCmdWithSubKeys(){
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "GEOADD","geoset1", "13.361389", "38.115556", "BJ", "15.087269", "37.502669", "SHA").toArray());
        Assert.assertEquals(RedisOpType.GEOADD, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
    }

    @Test
    public void testGTIDTransactionCmdWithSubKeys(){
        RedisOp multiRedisOp = parser.parse(Arrays.asList("MULTI").toArray());
        Assert.assertEquals(RedisOpType.MULTI, multiRedisOp.getOpType());
        RedisOp normalSetRedisOp = parser.parse(Arrays.asList("set","key1","val1").toArray());
        Assert.assertEquals(RedisOpType.SET,normalSetRedisOp.getOpType());
        RedisOp nomarlHMSetRedisOp = parser.parse(Arrays.asList("HMSET","hash1", "k1", "v1", "k2", "v2").toArray());
        Assert.assertEquals(RedisOpType.HMSET,nomarlHMSetRedisOp.getOpType());
        RedisOp execRedisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "exec").toArray());
        Assert.assertEquals("a1:10", execRedisOp.getOpGtid());
    }

    // ---------- 其他 FIELDS 型 Hash 命令测试 ----------

    @Test
    public void testGTIDHexPireCmds() {
        // HEXPIRE key seconds FIELDS numfields field
        RedisOp redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "HEXPIRE", "myhash", "100", "FIELDS", "1", "f1").toArray()
        );
        Assert.assertEquals(RedisOpType.HEXPIRE, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());

        // HEXPIREAT key unix-time-seconds FIELDS numfields field
        redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "HEXPIREAT", "myhash", "123456789", "FIELDS", "2", "f1", "f2").toArray()
        );
        Assert.assertEquals(RedisOpType.HEXPIREAT, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());

        // HPEXPIRE key milliseconds FIELDS numfields field
        redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "HPEXPIRE", "myhash", "5000", "FIELDS", "1", "f1").toArray()
        );
        Assert.assertEquals(RedisOpType.HPEXPIRE, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());

        // HPEXPIREAT key unix-time-milliseconds FIELDS numfields field
        redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "HPEXPIREAT", "myhash", "123456789000", "FIELDS", "1", "f1").toArray()
        );
        Assert.assertEquals(RedisOpType.HPEXPIREAT, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
    }

    @Test
    public void testGTIDHgetDelAndHgetex() {
        // HGETDEL key FIELDS numfields field
        RedisOp redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "HGETDEL", "myhash", "FIELDS", "1", "f1").toArray()
        );
        Assert.assertEquals(RedisOpType.HGETDEL, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());

        // HGETEX key PERSIST FIELDS numfields field
        redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "HGETEX", "myhash", "PERSIST", "FIELDS", "1", "f1").toArray()
        );
        Assert.assertEquals(RedisOpType.HGETEX, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());

        // HGETEX key EX 10 FIELDS 2 field1 field2
        redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "HGETEX", "myhash", "EX", "10", "FIELDS", "2", "f1", "f2").toArray()
        );
        Assert.assertEquals(RedisOpType.HGETEX, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
    }

    @Test
    public void testGTIDHPersist() {
        // HPERSIST key FIELDS numfields field
        RedisOp redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "HPERSIST", "myhash", "FIELDS", "2", "f1", "f2").toArray()
        );
        Assert.assertEquals(RedisOpType.HPERSIST, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
    }

    @Test
    public void testParseOtherHashFieldsCmds() {
        // HEXPIRE

        RedisOp redisOp = parser.parse(
                Arrays.asList("HEXPIRE", "myhash", "100", "FIELDS", "1", "f1").toArray()
        );
        Assert.assertEquals(RedisOpType.HEXPIRE, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());

        redisOp = parser.parse(
                Arrays.asList("HEXPIRE", "myhash", "100", "xx","FIELDS", "1", "f1").toArray()
        );
        Assert.assertEquals(RedisOpType.HEXPIRE, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());

        redisOp = parser.parse(
                Arrays.asList("HEXPIRE", "myhash", "100", "XX","FIELDS", "1", "f1").toArray()
        );
        Assert.assertEquals(RedisOpType.HEXPIRE, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());

        // HGETDEL
        redisOp = parser.parse(
                Arrays.asList("HGETDEL", "myhash", "FIELDS", "2", "f1", "f2").toArray()
        );
        Assert.assertEquals(RedisOpType.HGETDEL, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());

        // HPERSIST
        redisOp = parser.parse(
                Arrays.asList("HPERSIST", "myhash", "FIELDS", "1", "f1").toArray()
        );
        Assert.assertEquals(RedisOpType.HPERSIST, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
    }

    @Test
    public void testZMPOPParse() {
        RedisOp redisOp = parser.parse(
                Arrays.asList("ZMPOP", "2", "zset1", "zset2", "MIN", "COUNT", "5").toArray()
        );
        Assert.assertEquals(RedisOpType.ZMPOP, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());

        RedisMultiKeyOp multiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(2, multiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("zset1"), multiKeyOp.getKeyValue(0).getKey());
        Assert.assertNull(multiKeyOp.getKeyValue(0).getValue());
        Assert.assertEquals(new RedisKey("zset2"), multiKeyOp.getKeyValue(1).getKey());
        Assert.assertNull(multiKeyOp.getKeyValue(1).getValue());
    }

    @Test
    public void testZMPOPWithOneKey() {
        RedisOp redisOp = parser.parse(
                Arrays.asList("ZMPOP", "1", "zset1", "MAX").toArray()
        );
        Assert.assertEquals(RedisOpType.ZMPOP, redisOp.getOpType());

        RedisMultiKeyOp multiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(1, multiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("zset1"), multiKeyOp.getKeyValue(0).getKey());
    }

    @Test
    public void testBZMPOPParse() {
        RedisOp redisOp = parser.parse(
                Arrays.asList("BZMPOP", "0.5", "2", "zset1", "zset2", "MIN", "COUNT", "3").toArray()
        );
        Assert.assertEquals(RedisOpType.BZMPOP, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());

        RedisMultiKeyOp multiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(2, multiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("zset1"), multiKeyOp.getKeyValue(0).getKey());
        Assert.assertEquals(new RedisKey("zset2"), multiKeyOp.getKeyValue(1).getKey());
    }

    @Test
    public void testLMPOPParse() {
        RedisOp redisOp = parser.parse(
                Arrays.asList("LMPOP", "2", "list1", "list2", "LEFT", "COUNT", "10").toArray()
        );
        Assert.assertEquals(RedisOpType.LMPOP, redisOp.getOpType());

        RedisMultiKeyOp multiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(2, multiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("list1"), multiKeyOp.getKeyValue(0).getKey());
        Assert.assertEquals(new RedisKey("list2"), multiKeyOp.getKeyValue(1).getKey());
    }

    @Test
    public void testBLMPOPParse() {
        RedisOp redisOp = parser.parse(
                Arrays.asList("BLMPOP", "0.1", "2", "list1", "list2", "RIGHT").toArray()
        );
        Assert.assertEquals(RedisOpType.BLMPOP, redisOp.getOpType());

        RedisMultiKeyOp multiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(2, multiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("list1"), multiKeyOp.getKeyValue(0).getKey());
        Assert.assertEquals(new RedisKey("list2"), multiKeyOp.getKeyValue(1).getKey());
    }

    // ---------- 带 GTID 的测试 ----------
    @Test
    public void testGTIDZMPOPParse() {
        RedisOp redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "ZMPOP", "2", "zset1", "zset2", "MIN").toArray()
        );
        Assert.assertEquals(RedisOpType.ZMPOP, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());

        RedisMultiKeyOp multiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(2, multiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("zset1"), multiKeyOp.getKeyValue(0).getKey());
    }

    @Test
    public void testGTIDBZMPOPParse() {
        RedisOp redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "BZMPOP", "0.5", "1", "zset1", "MAX", "COUNT", "5").toArray()
        );
        Assert.assertEquals(RedisOpType.BZMPOP, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());

        RedisMultiKeyOp multiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(1, multiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("zset1"), multiKeyOp.getKeyValue(0).getKey());
    }

    @Test
    public void testGTIDLMPOPParse() {
        RedisOp redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "LMPOP", "3", "l1", "l2", "l3", "RIGHT", "COUNT", "2").toArray()
        );
        Assert.assertEquals(RedisOpType.LMPOP, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());

        RedisMultiKeyOp multiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(3, multiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("l1"), multiKeyOp.getKeyValue(0).getKey());
        Assert.assertEquals(new RedisKey("l2"), multiKeyOp.getKeyValue(1).getKey());
        Assert.assertEquals(new RedisKey("l3"), multiKeyOp.getKeyValue(2).getKey());
    }

    @Test
    public void testGTIDBLMPOPParse() {
        RedisOp redisOp = parser.parse(
                Arrays.asList("GTID", "a1:10", "0", "BLMPOP", "2", "2", "l1", "l2", "LEFT").toArray()
        );
        Assert.assertEquals(RedisOpType.BLMPOP, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());

        RedisMultiKeyOp multiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(2, multiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("l1"), multiKeyOp.getKeyValue(0).getKey());
        Assert.assertEquals(new RedisKey("l2"), multiKeyOp.getKeyValue(1).getKey());
    }
}
