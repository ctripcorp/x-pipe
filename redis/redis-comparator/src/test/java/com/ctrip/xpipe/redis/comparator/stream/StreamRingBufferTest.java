package com.ctrip.xpipe.redis.comparator.stream;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer.PeekStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StreamRingBufferTest extends AbstractTest {

    private static final File SOURCE = new File(
            "src/main/java/com/ctrip/xpipe/redis/comparator/stream/StreamRingBuffer.java");

    @Test
    public void testWriteFullOverwritesOldestWithoutBlockingOrClosingAutoRead() {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.config().setAutoRead(true);
        StreamRingBuffer buffer = new StreamRingBuffer(8, 0L);

        write(buffer, "abcdefgh".getBytes(StandardCharsets.US_ASCII));
        Assert.assertTrue(channel.config().isAutoRead());
        Assert.assertEquals(8L, buffer.getReceivedEnd());
        Assert.assertEquals(0L, buffer.getBufferStart());
        assertPeekHit(buffer, 0L, "abcdefgh".getBytes(StandardCharsets.US_ASCII));

        write(buffer, "ijkl".getBytes(StandardCharsets.US_ASCII));
        Assert.assertTrue(channel.config().isAutoRead());
        Assert.assertEquals(12L, buffer.getReceivedEnd());
        Assert.assertEquals(4L, buffer.getBufferStart());
        Assert.assertEquals(PeekStatus.OVERWRITTEN, peekStatus(buffer, 0L, 4));
        assertPeekHit(buffer, 4L, "efghijkl".getBytes(StandardCharsets.US_ASCII));
        channel.finish();
    }

    @Test
    public void testPeekSignalsDistinguishOverwrittenAndNotYet() {
        StreamRingBuffer buffer = new StreamRingBuffer(4, 100L);
        Assert.assertEquals(PeekStatus.NOT_YET, peekStatus(buffer, 100L, 1));
        Assert.assertEquals(PeekStatus.OVERWRITTEN, peekStatus(buffer, 99L, 1));

        write(buffer, new byte[]{1, 2, 3});
        Assert.assertEquals(PeekStatus.HIT, peekStatus(buffer, 100L, 3));
        Assert.assertEquals(PeekStatus.NOT_YET, peekStatus(buffer, 103L, 1));
        Assert.assertEquals(PeekStatus.NOT_YET, peekStatus(buffer, 101L, 3));

        write(buffer, new byte[]{4, 5, 6});
        Assert.assertEquals(106L, buffer.getReceivedEnd());
        Assert.assertEquals(102L, buffer.getBufferStart());
        Assert.assertEquals(PeekStatus.OVERWRITTEN, peekStatus(buffer, 100L, 1));
        Assert.assertEquals(PeekStatus.OVERWRITTEN, peekStatus(buffer, 101L, 2));
        Assert.assertEquals(PeekStatus.HIT, peekStatus(buffer, 102L, 4));
        Assert.assertEquals(PeekStatus.NOT_YET, peekStatus(buffer, 106L, 1));
    }

    @Test
    public void testWrapAroundPeekMatchesLinearBuffer() throws Exception {
        int capacity = 8;
        long streamStart = 50L;
        StreamRingBuffer ring = new StreamRingBuffer(capacity, streamStart);
        ByteArrayOutputStream linear = new ByteArrayOutputStream();

        byte[][] chunks = new byte[][]{
                bytes(1, 2, 3, 4, 5),
                bytes(6, 7, 8, 9),
                bytes(10, 11, 12, 13, 14, 15, 16),
                bytes(17, 18)
        };
        for (byte[] chunk : chunks) {
            write(ring, chunk);
            linear.write(chunk);
            assertWindowMatchesLinear(ring, linear.toByteArray(), streamStart);
        }
    }

    @Test
    public void testReceivedEndPublishHidesUnpublishedBytes() {
        StreamRingBuffer buffer = new StreamRingBuffer(4, 0L);
        write(buffer, new byte[]{10, 20, 30, 40});
        write(buffer, new byte[]{50, 60});
        Assert.assertEquals(6L, buffer.getReceivedEnd());
        Assert.assertEquals(2L, buffer.getBufferStart());
        assertPeekHit(buffer, 2L, new byte[]{30, 40, 50, 60});

        byte[] reuse = new byte[1];
        Assert.assertEquals(PeekStatus.NOT_YET, buffer.peek(6L, 1, reuse));
        Assert.assertEquals((byte) 30, buffer.peek(2L, 1, reuse) == PeekStatus.HIT ? reuse[0] : -1);
    }

    @Test(timeout = 10000)
    public void testSingleWriterSingleReaderSeesOnlyPublishedBytes() throws Exception {
        int capacity = 64;
        int total = 8000;
        StreamRingBuffer buffer = new StreamRingBuffer(capacity, 0L);
        AtomicReference<Throwable> readerError = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        Thread reader = new Thread(() -> {
            try {
                byte[] reuse = new byte[1];
                long next = 0L;
                while (next < total) {
                    PeekStatus status = buffer.peek(next, 1, reuse);
                    if (status == PeekStatus.OVERWRITTEN) {
                        next = buffer.getBufferStart();
                        continue;
                    }
                    if (status == PeekStatus.NOT_YET) {
                        Thread.yield();
                        continue;
                    }
                    byte expected = (byte) next;
                    if (reuse[0] != expected) {
                        throw new AssertionError("offset " + next + " expected " + expected
                                + " got " + reuse[0]);
                    }
                    next++;
                }
            } catch (Throwable t) {
                readerError.set(t);
            } finally {
                done.countDown();
            }
        }, "ring-reader");
        reader.setDaemon(true);
        reader.start();

        for (int i = 0; i < total; i++) {
            write(buffer, new byte[]{(byte) i});
        }
        Assert.assertTrue(done.await(5, TimeUnit.SECONDS));
        Assert.assertNull(readerError.get());
        reader.join(1000);
    }

    @Test
    public void testNoSkipOrSkipUntil() throws Exception {
        Assert.assertTrue(SOURCE.isFile());
        String text = new String(Files.readAllBytes(SOURCE.toPath()), StandardCharsets.UTF_8);
        Assert.assertFalse(text.contains("void skip(") || text.contains("skipUntil("));
        Assert.assertFalse(text.contains("setAutoRead"));
        Assert.assertFalse(text.contains("CompositeByteBuf"));
        Assert.assertFalse(text.contains(".retain("));

        for (Method method : StreamRingBuffer.class.getDeclaredMethods()) {
            Assert.assertFalse("must not expose " + method.getName(),
                    method.getName().equals("skip") || method.getName().equals("skipUntil"));
        }
    }

    @Test
    public void testCapacityFixedToConfigNotScaledByStreamCount() {
        ComparatorConfig config = new ComparatorConfig();
        StreamRingBuffer first = new StreamRingBuffer(config, 0L);
        StreamRingBuffer second = new StreamRingBuffer(config, 1000L);
        Assert.assertEquals(ComparatorConfig.DEFAULT_STREAM_BUFFER_BYTES, first.getCapacity());
        Assert.assertEquals(first.getCapacity(), second.getCapacity());
        Assert.assertEquals(config.getStreamBufferBytes(), first.getCapacity());
        Assert.assertEquals(0L, first.getStreamStart());
        Assert.assertEquals(1000L, second.getStreamStart());
    }

    @Test
    public void testWriteDoesNotRetainDirectBuf() {
        StreamRingBuffer buffer = new StreamRingBuffer(8, 0L);
        ByteBuf direct = Unpooled.directBuffer(4);
        try {
            direct.writeBytes(new byte[]{1, 2, 3, 4});
            int ref = direct.refCnt();
            buffer.write(direct);
            Assert.assertEquals(ref, direct.refCnt());
            Assert.assertEquals(0, direct.readableBytes());
            assertPeekHit(buffer, 0L, new byte[]{1, 2, 3, 4});
        } finally {
            direct.release();
        }

        CompositeByteBuf composite = Unpooled.compositeBuffer();
        try {
            composite.addComponent(true, Unpooled.wrappedBuffer(new byte[]{5, 6}));
            int ref = composite.refCnt();
            buffer.write(composite);
            Assert.assertEquals(ref, composite.refCnt());
            Assert.assertEquals(0, composite.readableBytes());
            assertPeekHit(buffer, 4L, new byte[]{5, 6});
        } finally {
            composite.release();
        }
    }

    @Test
    public void testWriteConsumesReadableBytesSoRetryDoesNotDuplicate() {
        StreamRingBuffer buffer = new StreamRingBuffer(16, 100L);
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        buffer.write(buf);
        buffer.write(buf);
        Assert.assertEquals(0, buf.readableBytes());
        Assert.assertEquals(103L, buffer.getReceivedEnd());
        assertPeekHit(buffer, 100L, new byte[]{1, 2, 3});
    }

    private static void write(StreamRingBuffer buffer, byte[] bytes) {
        buffer.write(Unpooled.wrappedBuffer(bytes));
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }

    private static PeekStatus peekStatus(StreamRingBuffer buffer, long offset, int len) {
        return buffer.peek(offset, len, new byte[len]);
    }

    private static void assertPeekHit(StreamRingBuffer buffer, long offset, byte[] expected) {
        byte[] reuse = new byte[expected.length];
        Assert.assertEquals(PeekStatus.HIT, buffer.peek(offset, expected.length, reuse));
        Assert.assertArrayEquals(expected, reuse);
    }

    private static void assertWindowMatchesLinear(StreamRingBuffer ring, byte[] allWritten, long streamStart) {
        long start = ring.getBufferStart();
        long end = ring.getReceivedEnd();
        int len = (int) (end - start);
        byte[] fromRing = new byte[len];
        if (len > 0) {
            Assert.assertEquals(PeekStatus.HIT, ring.peek(start, len, fromRing));
        }
        int from = (int) (start - streamStart);
        int to = (int) (end - streamStart);
        byte[] fromLinear = java.util.Arrays.copyOfRange(allWritten, from, to);
        Assert.assertArrayEquals(fromLinear, fromRing);
        Assert.assertEquals(Math.max(streamStart, end - ring.getCapacity()), start);

        if (len >= 2) {
            int wrapLen = Math.min(len, ring.getCapacity());
            long wrapOffset = end - wrapLen;
            if (wrapOffset < start) {
                wrapOffset = start;
                wrapLen = (int) (end - wrapOffset);
            }
            byte[] wrapPeek = new byte[wrapLen];
            Assert.assertEquals(PeekStatus.HIT, ring.peek(wrapOffset, wrapLen, wrapPeek));
            int wrapFrom = (int) (wrapOffset - streamStart);
            Assert.assertArrayEquals(
                    java.util.Arrays.copyOfRange(allWritten, wrapFrom, wrapFrom + wrapLen),
                    wrapPeek);
        }
    }
}
