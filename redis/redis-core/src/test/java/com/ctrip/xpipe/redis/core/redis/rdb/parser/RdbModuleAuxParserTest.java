package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbModuleAuxDataBytes.redisSearchModuleAuxRdbBytes;

/**
 * MODULE_AUX (247) consume/skip tests for Applier RDB full sync.
 */
public class RdbModuleAuxParserTest extends AbstractTest implements RdbParseListener {

    /** Typical RediSearch IndexSpecType id needs full 64-bit len (0x81 + 8 bytes). */
    private static final long SAMPLE_64BIT_MODULE_ID = 0x1234567890ABCDEFL;
    /** Forces 32-bit len encoding (0x80 + 4 bytes): >= 2^14 and <= UINT32_MAX. */
    private static final long SAMPLE_32BIT_MODULE_ID = 1L << 20; // 1048576

    private DefaultRdbParser parser;
    private DefaultRdbParseContext context;
    private List<RedisOp> redisOps;

    @Before
    public void setUp() {
        context = new DefaultRdbParseContext();
        parser = new DefaultRdbParser(context);
        parser.registerListener(this);
        parser.needFinishNotify(true);
        redisOps = new ArrayList<>();
    }

    @Test
    public void testParseModuleAuxThenStringKey() throws Exception {
        byte[] rdb = buildRdbWithModuleAuxAndStringKey(1L, "ab");
        ByteBuf byteBuf = Unpooled.wrappedBuffer(rdb);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals(0, byteBuf.readableBytes());
        Assert.assertEquals(0, context.getDbId());
        Assert.assertEquals(2, redisOps.size());
        Assert.assertEquals("SELECT 0", redisOps.get(0).toString());
        Assert.assertEquals("SET k1 v1", redisOps.get(1).toString());
    }

    @Test
    public void testParseModuleAuxChunked() throws Exception {
        byte[] rdb = buildRdbWithModuleAuxAndStringKey(1L, "ab");
        ByteBuf byteBuf = Unpooled.wrappedBuffer(rdb);

        while (!parser.isFinish() && byteBuf.isReadable()) {
            ByteBuf one = byteBuf.readSlice(1);
            parser.read(one);
        }

        Assert.assertTrue(parser.isFinish());
        Assert.assertEquals(2, redisOps.size());
        Assert.assertEquals("SET k1 v1", redisOps.get(1).toString());
    }

    @Test
    public void testModuleAuxParserStandalone() throws Exception {
        DefaultRdbParseContext ctx = new DefaultRdbParseContext();
        RdbModuleAuxParser auxParser = new RdbModuleAuxParser(ctx);

        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeLen(payload, 1); // moduleId 6-bit
        writeLen(payload, RdbConstant.RDB_MODULE_OPCODE_UINT);
        writeLen(payload, 1); // when = BEFORE_RDB
        writeLen(payload, RdbConstant.RDB_MODULE_OPCODE_UINT);
        writeLen(payload, 42);
        writeLen(payload, RdbConstant.RDB_MODULE_OPCODE_STRING);
        writeRawString(payload, "hi");
        writeLen(payload, RdbConstant.RDB_MODULE_OPCODE_EOF);

        ByteBuf buf = Unpooled.wrappedBuffer(payload.toByteArray());
        Integer done = null;
        while (done == null) {
            done = auxParser.read(buf);
        }
        Assert.assertEquals(Integer.valueOf(1), done);
        Assert.assertTrue(auxParser.isFinish());
        Assert.assertEquals(0, buf.readableBytes());
    }

    @Test
    public void testModuleAuxWith64BitModuleIdAndLongString() throws Exception {
        char[] chars = new char[80];
        Arrays.fill(chars, 'x');
        String longStr = new String(chars); // >63 → 14-bit string len

        byte[] rdb = buildRdbWithModuleAuxAndStringKey(SAMPLE_64BIT_MODULE_ID, longStr);
        // sanity: 64-bit moduleId encoding prefix
        Assert.assertTrue(containsSubsequence(rdb, new byte[]{
                (byte) RdbConstant.REDIS_RDB_OP_CODE_MODULE_AUX,
                (byte) RdbConstant.REDIS_RDB_LEN_64BITLEN
        }));

        ByteBuf byteBuf = Unpooled.wrappedBuffer(rdb);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals(0, byteBuf.readableBytes());
        Assert.assertEquals(2, redisOps.size());
        Assert.assertEquals("SET k1 v1", redisOps.get(1).toString());
    }

    @Test
    public void testModuleAuxWith32BitModuleIdChunked() throws Exception {
        char[] chars = new char[100];
        Arrays.fill(chars, 'y');
        String longStr = new String(chars);

        byte[] rdb = buildRdbWithModuleAuxAndStringKey(SAMPLE_32BIT_MODULE_ID, longStr);
        Assert.assertTrue(containsSubsequence(rdb, new byte[]{
                (byte) RdbConstant.REDIS_RDB_OP_CODE_MODULE_AUX,
                (byte) RdbConstant.REDIS_RDB_LEN_32BITLEN
        }));

        ByteBuf byteBuf = Unpooled.wrappedBuffer(rdb);
        while (!parser.isFinish() && byteBuf.isReadable()) {
            parser.read(byteBuf.readSlice(1));
        }

        Assert.assertTrue(parser.isFinish());
        Assert.assertEquals(2, redisOps.size());
        Assert.assertEquals("SET k1 v1", redisOps.get(1).toString());
    }

    @Test
    public void testParseRealRedisSearchRdbWithModuleAux() {
        // Real Redis 8.10 + RediSearch dump (blog_idx MODULE_AUX + HASH docs), captured as byte[].
        Assert.assertTrue(containsSubsequence(redisSearchModuleAuxRdbBytes, new byte[]{
                (byte) RdbConstant.REDIS_RDB_OP_CODE_MODULE_AUX,
                (byte) RdbConstant.REDIS_RDB_LEN_64BITLEN
        }));

        ByteBuf byteBuf = Unpooled.wrappedBuffer(redisSearchModuleAuxRdbBytes);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertTrue(parser.isFinish());
        // EOF 后剩余 8 字节 CRC，DefaultRdbParser 不消费（与 DefaultRdbParserTest 一致）
        Assert.assertEquals(8, byteBuf.readableBytes());
        Assert.assertEquals("8.10.0", context.getAux("redis-ver"));
        Assert.assertEquals(0, context.getDbId());

        List<String> ops = redisOps.stream().map(RedisOp::toString).collect(Collectors.toList());
        Assert.assertTrue(ops.contains("SELECT 0"));
        Assert.assertTrue(ops.stream().anyMatch(op -> op.startsWith("HSET blog:")));
        // MODULE_AUX is skipped: no FT.* op, but subsequent hash keys still parsed
        Assert.assertTrue(ops.stream().noneMatch(op -> op.startsWith("FT.")));
    }

    @Test
    public void testParseRealRedisSearchRdbWithModuleAuxChunked() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(redisSearchModuleAuxRdbBytes);
        while (!parser.isFinish() && byteBuf.isReadable()) {
            parser.read(byteBuf.readSlice(1));
        }

        Assert.assertTrue(parser.isFinish());
        Assert.assertEquals(8, byteBuf.readableBytes());
        Assert.assertTrue(redisOps.stream().map(RedisOp::toString).anyMatch(op -> op.startsWith("HSET blog:")));
    }

    @Test
    public void testModuleAuxStandalone64BitModuleId() throws Exception {
        DefaultRdbParseContext ctx = new DefaultRdbParseContext();
        RdbModuleAuxParser auxParser = new RdbModuleAuxParser(ctx);

        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeLen(payload, SAMPLE_64BIT_MODULE_ID);
        writeLen(payload, RdbConstant.RDB_MODULE_OPCODE_UINT);
        writeLen(payload, 1);
        writeLen(payload, RdbConstant.RDB_MODULE_OPCODE_STRING);
        char[] chars = new char[70];
        Arrays.fill(chars, 'z');
        writeRawString(payload, new String(chars));
        writeLen(payload, RdbConstant.RDB_MODULE_OPCODE_SINT);
        writeLen(payload, 7);
        writeLen(payload, RdbConstant.RDB_MODULE_OPCODE_EOF);

        byte[] bytes = payload.toByteArray();
        Assert.assertEquals(RdbConstant.REDIS_RDB_LEN_64BITLEN, bytes[0] & 0xff);

        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        Integer done = null;
        while (done == null) {
            done = auxParser.read(buf);
        }
        Assert.assertTrue(auxParser.isFinish());
        Assert.assertEquals(0, buf.readableBytes());
    }

    private static byte[] buildRdbWithModuleAuxAndStringKey(long moduleId, String moduleString)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write("REDIS0015".getBytes(StandardCharsets.US_ASCII));

        out.write(RdbConstant.REDIS_RDB_OP_CODE_MODULE_AUX & 0xff);
        writeLen(out, moduleId);
        writeLen(out, RdbConstant.RDB_MODULE_OPCODE_UINT);
        writeLen(out, 1); // when = BEFORE_RDB
        writeLen(out, RdbConstant.RDB_MODULE_OPCODE_UINT);
        writeLen(out, 100);
        writeLen(out, RdbConstant.RDB_MODULE_OPCODE_STRING);
        writeRawString(out, moduleString);
        writeLen(out, RdbConstant.RDB_MODULE_OPCODE_FLOAT);
        out.write(new byte[]{0, 0, 0, 0});
        writeLen(out, RdbConstant.RDB_MODULE_OPCODE_DOUBLE);
        out.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
        writeLen(out, RdbConstant.RDB_MODULE_OPCODE_EOF);

        out.write(RdbConstant.REDIS_RDB_OP_CODE_SELECTDB & 0xff);
        writeLen(out, 0);

        out.write(RdbConstant.REDIS_RDB_TYPE_STRING & 0xff);
        writeRawString(out, "k1");
        writeRawString(out, "v1");

        out.write(RdbConstant.REDIS_RDB_OP_CODE_EOF & 0xff);
        return out.toByteArray();
    }

    /**
     * Encode length the same way as redis rdbSaveLen().
     */
    private static void writeLen(ByteArrayOutputStream out, long value) throws IOException {
        if (value < 0) {
            throw new IllegalArgumentException("len must be non-negative: " + value);
        }
        if (value < (1L << 6)) {
            out.write((int) (value & 0xFF));
        } else if (value < (1L << 14)) {
            out.write((int) (((value >> 8) & 0xFF) | (RdbConstant.REDIS_RDB_LEN_14BITLEN << 6)));
            out.write((int) (value & 0xFF));
        } else if (value <= 0xFFFFFFFFL) {
            out.write(RdbConstant.REDIS_RDB_LEN_32BITLEN & 0xff);
            out.write((int) ((value >> 24) & 0xFF));
            out.write((int) ((value >> 16) & 0xFF));
            out.write((int) ((value >> 8) & 0xFF));
            out.write((int) (value & 0xFF));
        } else {
            out.write(RdbConstant.REDIS_RDB_LEN_64BITLEN & 0xff);
            for (int shift = 56; shift >= 0; shift -= 8) {
                out.write((int) ((value >> shift) & 0xFF));
            }
        }
    }

    private static void writeRawString(ByteArrayOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.US_ASCII);
        writeLen(out, bytes.length);
        out.write(bytes);
    }

    private static boolean containsSubsequence(byte[] haystack, byte[] needle) {
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            if (Arrays.equals(haystack, i, i + needle.length, needle, 0, needle.length)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {
        redisOps.add(redisOp);
    }

    @Override
    public void onAux(String key, String value) {
    }

    @Override
    public void onFinish(RdbParser<?> parser) {
    }

    @Override
    public void onAuxFinish(Map<String, String> auxMap) {
    }
}
