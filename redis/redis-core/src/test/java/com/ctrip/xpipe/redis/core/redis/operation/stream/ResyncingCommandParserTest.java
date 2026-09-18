package com.ctrip.xpipe.redis.core.redis.operation.stream;

import com.ctrip.xpipe.payload.DirectByteBufInOutPayload;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ResyncingCommandParserTest {

    private final List<CapturedCommand> captured = new ArrayList<>();
    private ResyncingCommandParser parser;
    private final List<ByteBuf> toRelease = new ArrayList<>();

    @Before
    public void setUp() {
        captured.clear();
        parser = new ResyncingCommandParser((payload, commandBuf) -> {
            String[] args = payloadToStringArray(payload);
            byte[] raw = new byte[commandBuf.readableBytes()];
            commandBuf.getBytes(commandBuf.readerIndex(), raw);
            captured.add(new CapturedCommand(args, raw));
        });
    }

    @After
    public void tearDown() {
        parser.reset();
        for (ByteBuf buf : toRelease) {
            if (buf.refCnt() > 0) {
                buf.release(buf.refCnt());
            }
        }
        toRelease.clear();
    }

    @Test
    public void alignFromArbitraryOffset() throws IOException {
        ByteBuf command = arraysRepl("set", "key1", "value1");
        String dirty = "hello\r\nworld\r\nxx";
        ByteBuf buf = Unpooled.buffer(dirty.length() + command.readableBytes());
        track(buf);
        buf.writeBytes(dirty.getBytes(StandardCharsets.UTF_8));
        buf.writeBytes(command);

        parser.doRead(buf);

        Assert.assertEquals(1, captured.size());
        assertSetCommand(captured.get(0), "key1", "value1");
    }

    @Test
    public void syntaxErrorResyncsFromNextByteWithoutDroppingRest() throws IOException {
        ByteBuf valid = arraysRepl("set", "key1", "value1");
        // Same dirty span as StreamCommandParserTest.doRead_abnormalTest: parse fails
        // mid-command, then a complete command follows in the same / next buffer.
        ByteBuf first = wrapUtf8("*3\r\n$3\r\nval$2");
        parser.doRead(first);
        Assert.assertEquals(0, captured.size());

        ByteBuf second = Unpooled.buffer();
        track(second);
        second.writeBytes("ue1\r\n".getBytes(StandardCharsets.UTF_8));
        second.writeBytes(valid);

        parser.doRead(second);

        Assert.assertEquals(1, captured.size());
        assertSetCommand(captured.get(0), "key1", "value1");
    }

    @Test
    public void syntaxErrorInSameBufferKeepsFollowingCommand() throws IOException {
        ByteBuf valid = arraysRepl("publish", "ch", "hello");
        ByteBuf buf = Unpooled.buffer();
        track(buf);
        buf.writeBytes("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalueX\r\n".getBytes(StandardCharsets.UTF_8));
        buf.writeBytes(valid);

        parser.doRead(buf);

        Assert.assertEquals(1, captured.size());
        Assert.assertEquals("publish", captured.get(0).args[0].toLowerCase());
        Assert.assertEquals("ch", captured.get(0).args[1]);
        Assert.assertEquals("hello", captured.get(0).args[2]);
    }

    @Test
    public void crossChunkRemnantStitches() throws IOException {
        ByteBuf whole = arraysRepl("set", "key1", "value1");
        int split = 10;
        Assert.assertTrue(split < whole.readableBytes());

        ByteBuf part1 = whole.readSlice(split);
        parser.doRead(part1);
        Assert.assertEquals(0, captured.size());
        Assert.assertTrue(parser.getRemainLength() > 0);

        ByteBuf part2 = whole.slice(whole.readerIndex(), whole.readableBytes());
        parser.doRead(part2);

        Assert.assertEquals(1, captured.size());
        assertSetCommand(captured.get(0), "key1", "value1");
        Assert.assertEquals(0, parser.getRemainLength());
    }

    @Test
    public void incompleteHeaderAcrossChunks() throws IOException {
        ByteBuf headerHead = wrapUtf8("*3\r");
        parser.doRead(headerHead);
        Assert.assertEquals(0, captured.size());
        Assert.assertTrue(parser.getRemainLength() > 0);

        // remain is "*3\r"; skip that prefix so this chunk starts at "\n..."
        ByteBuf whole = arraysRepl("set", "k", "v");
        whole.skipBytes(3);
        parser.doRead(whole);

        Assert.assertEquals(1, captured.size());
        assertSetCommand(captured.get(0), "k", "v");
    }

    @Test
    public void refCntUnchangedAfterCompleteCommand() throws IOException {
        ByteBuf buf = arraysRepl("set", "key1", "value1");
        int before = buf.refCnt();

        parser.doRead(buf);

        Assert.assertEquals(1, captured.size());
        Assert.assertEquals(before, buf.refCnt());
        parser.reset();
        Assert.assertEquals(before, buf.refCnt());
    }

    @Test
    public void refCntRestoredAfterIncompleteThenComplete() throws IOException {
        ByteBuf whole = arraysRepl("set", "key1", "value1");
        ByteBuf chunk1 = Unpooled.copiedBuffer(whole.slice(0, 10));
        track(chunk1);
        ByteBuf chunk2 = Unpooled.copiedBuffer(whole.slice(10, whole.readableBytes() - 10));
        track(chunk2);
        int chunk1Before = chunk1.refCnt();
        int chunk2Before = chunk2.refCnt();

        parser.doRead(chunk1);
        Assert.assertEquals(0, captured.size());

        parser.doRead(chunk2);
        Assert.assertEquals(1, captured.size());
        Assert.assertEquals(chunk1Before, chunk1.refCnt());
        Assert.assertEquals(chunk2Before, chunk2.refCnt());

        parser.reset();
        Assert.assertEquals(chunk1Before, chunk1.refCnt());
        Assert.assertEquals(chunk2Before, chunk2.refCnt());
    }

    @Test
    public void resetReleasesIncompleteRemnant() throws IOException {
        ByteBuf partial = wrapUtf8("*3\r\n$3\r\nSET\r\n$");
        int before = partial.refCnt();

        parser.doRead(partial);
        Assert.assertTrue(parser.getRemainLength() > 0);

        parser.reset();
        Assert.assertEquals(0, parser.getRemainLength());
        Assert.assertEquals(before, partial.refCnt());
    }

    @Test
    public void twoCommandsInOneBuffer() throws IOException {
        ByteBuf buf = Unpooled.buffer();
        track(buf);
        buf.writeBytes(arraysRepl("set", "a", "1"));
        buf.writeBytes(arraysRepl("set", "b", "2"));

        parser.doRead(buf);

        Assert.assertEquals(2, captured.size());
        assertSetCommand(captured.get(0), "a", "1");
        assertSetCommand(captured.get(1), "b", "2");
    }

    /**
     * Redis values are binary-safe: SET k1 with payload {@code xxx*3\r\nxxx} must stay
     * one command. PARSING uses bulk-string length, so the decoy header inside the
     * value must not split the command or steal the following one.
     */
    @Test
    public void decoyHeaderInsideValueDoesNotSplitCommand() throws IOException {
        String decoyValue = "xxx*3\r\nxxx";
        ByteBuf buf = Unpooled.buffer();
        track(buf);
        buf.writeBytes(arraysRepl("set", "k1", decoyValue));
        buf.writeBytes(arraysRepl("publish", "ch", "hello"));

        parser.doRead(buf);

        Assert.assertEquals(2, captured.size());
        assertSetCommand(captured.get(0), "k1", decoyValue);
        Assert.assertEquals("publish", captured.get(1).args[0].toLowerCase());
        Assert.assertEquals("ch", captured.get(1).args[1]);
        Assert.assertEquals("hello", captured.get(1).args[2]);
    }

    /**
     * SEEKING from a dirty prefix that itself contains {@code *3\r\n} (the same
     * bytes a real array header would have) must fail that candidate and still
     * align on the following real command.
     */
    @Test
    public void decoyHeaderInDirtyPrefixDoesNotStealRealCommand() throws IOException {
        ByteBuf command = arraysRepl("set", "k1", "value1");
        String dirty = "xxx*3\r\nxxx";
        ByteBuf buf = Unpooled.buffer(dirty.length() + command.readableBytes());
        track(buf);
        buf.writeBytes(dirty.getBytes(StandardCharsets.UTF_8));
        buf.writeBytes(command);

        parser.doRead(buf);

        Assert.assertEquals(1, captured.size());
        assertSetCommand(captured.get(0), "k1", "value1");
    }

    /**
     * Mid-command start at the decoy {@code *3\r\n} inside a bulk value: that
     * candidate is not a real command head, so the next complete command after
     * the unfinished value must still be found.
     */
    @Test
    public void startAtDecoyInsideValueFindsNextCommand() throws IOException {
        ByteBuf first = arraysRepl("set", "k1", "xxx*3\r\nxxx");
        ByteBuf second = arraysRepl("set", "k2", "v2");
        byte[] needle = "*3\r\n".getBytes(StandardCharsets.UTF_8);
        int decoyAt = indexOfBytes(first, needle, first.readerIndex() + 1);
        Assert.assertTrue("decoy *3\\r\\n should appear inside the SET value", decoyAt > 0);

        ByteBuf buf = Unpooled.buffer();
        track(buf);
        first.readerIndex(decoyAt);
        buf.writeBytes(first);
        buf.writeBytes(second);

        parser.doRead(buf);

        Assert.assertEquals(1, captured.size());
        assertSetCommand(captured.get(0), "k2", "v2");
    }

    private void assertSetCommand(CapturedCommand cmd, String key, String value) {
        Assert.assertEquals(3, cmd.args.length);
        Assert.assertEquals("set", cmd.args[0].toLowerCase());
        Assert.assertEquals(key, cmd.args[1]);
        Assert.assertEquals(value, cmd.args[2]);
        Assert.assertTrue(cmd.raw.length > 0);
        Assert.assertEquals('*', cmd.raw[0]);
    }

    private ByteBuf wrapUtf8(String s) {
        ByteBuf buf = Unpooled.copiedBuffer(s, StandardCharsets.UTF_8);
        track(buf);
        return buf;
    }

    private ByteBuf arraysRepl(String... expected) {
        ByteBuf buffer = Unpooled.buffer();
        track(buffer);
        buffer.writeByte(RedisClientProtocol.ASTERISK_BYTE);
        buffer.writeBytes(String.valueOf(expected.length).getBytes(StandardCharsets.UTF_8));
        buffer.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        for (String s : expected) {
            buffer.writeByte(RedisClientProtocol.DOLLAR_BYTE);
            buffer.writeBytes(String.valueOf(s.length()).getBytes(StandardCharsets.UTF_8));
            buffer.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
            buffer.writeBytes(s.getBytes(StandardCharsets.UTF_8));
            buffer.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        return buffer;
    }

    private ByteBuf track(ByteBuf buf) {
        toRelease.add(buf);
        return buf;
    }

    private static int indexOfBytes(ByteBuf buf, byte[] needle, int from) {
        int end = buf.writerIndex() - needle.length;
        for (int i = from; i <= end; i++) {
            boolean match = true;
            for (int j = 0; j < needle.length; j++) {
                if (buf.getByte(i + j) != needle[j]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return i;
            }
        }
        return -1;
    }

    private static String[] payloadToStringArray(Object[] payload) {
        String[] res = new String[payload.length];
        for (int i = 0; i < payload.length; i++) {
            if (payload[i] instanceof DirectByteBufInOutPayload) {
                res[i] = payload[i].toString();
            } else if (payload[i] != null) {
                res[i] = payload[i].toString();
            }
        }
        return res;
    }

    private static class CapturedCommand {
        final String[] args;
        final byte[] raw;

        CapturedCommand(String[] args, byte[] raw) {
            this.args = args;
            this.raw = raw;
        }

        @Override
        public String toString() {
            return Arrays.toString(args) + " rawLen=" + raw.length;
        }
    }
}
