package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.AbstractTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;

import static com.ctrip.xpipe.redis.proxy.handler.ZstdConstants.DEFAULT_BLOCK_SIZE;
import static com.ctrip.xpipe.redis.proxy.handler.ZstdConstants.HEADER_LENGTH;
import static com.ctrip.xpipe.redis.proxy.handler.ZstdConstants.MIN_BLOCK_SIZE;


/**
 * To whom might change ZstdDecoder,
 * Please ensure that ZstdEncoder & ZstdEncoderTest is valid before you do any change to decoder
 * As this Unit Test is critically dependent by ZstdEncoderTest's correctness
 * */
public class ZstdDecoderTest extends AbstractTest {

    private EmbeddedChannel channel = new EmbeddedChannel(new ZstdEncoder(), new ZstdDecoder());

    @After
    public void afterZstdDecoderTest() throws InterruptedException {
        channel.close().sync();
    }

    @Test
    public void testDecodeWithOneBlockSize() {

        testTemplate(DEFAULT_BLOCK_SIZE);
    }

    @Test
    public void testDecodeWithUncompressedSize() {
        testTemplate(MIN_BLOCK_SIZE);
    }

    @Test
    public void testLittleCompressChunk() {
        testTemplate(MIN_BLOCK_SIZE + 2);
    }

    @Test
    public void testCompositeCompressedChunk() {
        testTemplate(DEFAULT_BLOCK_SIZE + MIN_BLOCK_SIZE + 2);
    }

    @Test
    public void testCompressedChunkPlusUncompressedChunk() {
        testTemplate(DEFAULT_BLOCK_SIZE * 2 + MIN_BLOCK_SIZE);
    }

    @Test
    public void testChaosCutString() {
        testTemplate(DEFAULT_BLOCK_SIZE + MIN_BLOCK_SIZE - 1);
        testTemplate(DEFAULT_BLOCK_SIZE);
        testTemplate(MIN_BLOCK_SIZE + 3);
    }

    @Test
    public void testTcpPacketSplitFraming() {
        int N = randomInt(5, 10);

        ByteBuf decompressed = Unpooled.buffer(MIN_BLOCK_SIZE + 16);
        String sample = randomString(MIN_BLOCK_SIZE + 16);
        ByteBuf compressed = getCompressedByteBuf(sample);

        while(compressed.isReadable()) {
            ByteBuf piece = compressed.readRetainedSlice(Math.min(randomInt(1, N), compressed.readableBytes()));
            channel.writeInbound(piece);
        }

        mergeOutput(decompressed);

        Assert.assertEquals(sample, decompressed.toString(Charset.defaultCharset()));

    }

    private void mergeOutput(ByteBuf decompressed) {
        ByteBuf output;
        while((output = channel.readInbound()) != null) {
            Assert.assertNotNull(output);
            decompressed.ensureWritable(output.writableBytes());
            decompressed.writeBytes(output);
            output.release();
        }
    }


    @Test
    public void testTcpPacketMergedFraming() {

        ByteBuf decompressed = Unpooled.buffer(MIN_BLOCK_SIZE * 2 + 16);
        String sample1 = randomString(MIN_BLOCK_SIZE + 16);
        ByteBuf compressed1 = getCompressedByteBuf(sample1);

        String sample2 = randomString(MIN_BLOCK_SIZE);
        ByteBuf compressed2 = getCompressedByteBuf(sample2);

        int expectLength = compressed1.readableBytes() + compressed2.readableBytes();
        int totalLength = 0;
        int randomLength = randomInt(1, compressed1.readableBytes() - 3);
        totalLength += randomLength;
        ByteBuf piece = compressed1.readRetainedSlice(randomLength);
        channel.writeInbound(piece);

        randomLength = randomInt(HEADER_LENGTH + 5, compressed2.readableBytes() - 3);
        piece = Unpooled.directBuffer(compressed1.readableBytes() + randomLength);
        piece.writeBytes(compressed1);
        piece.writeBytes(compressed2.readSlice(randomLength));

        totalLength += piece.readableBytes();
        channel.writeInbound(piece);

        totalLength += compressed2.readableBytes();
        channel.writeInbound(compressed2);

        Assert.assertEquals(expectLength, totalLength);

        mergeOutput(decompressed);

        Assert.assertEquals(sample1 + sample2, decompressed.toString(Charset.defaultCharset()));

    }

    @Test
    public void testMultiThread() {

    }

    private ByteBuf getCompressedByteBuf(String sample) {

        ByteBuf byteBuf = getInputByteBuf(sample);

        Assert.assertTrue(channel.writeOutbound(byteBuf));

        ByteBuf compressed = channel.readOutbound();
        Assert.assertNotNull(compressed);
        Assert.assertTrue(compressed.isReadable());

        return compressed;
    }

    private void testTemplate(int length) {
        String sample = randomString(length);
        ByteBuf compressed = getCompressedByteBuf(sample);

        Assert.assertTrue(channel.writeInbound(compressed));

        ByteBuf decompressed = Unpooled.buffer(length);
        ByteBuf output;
        mergeOutput(decompressed);


        String result = decompressed.toString(Charset.defaultCharset());
        Assert.assertEquals(sample.length(), result.length());
        Assert.assertEquals(sample, result);
    }

    private ByteBuf getInputByteBuf(String sample) {
        ByteBuf byteBuf = Unpooled.directBuffer(DEFAULT_BLOCK_SIZE);
        byte[] sampleBytes = sample.getBytes();
        byteBuf.ensureWritable(sampleBytes.length);
        byteBuf.writeBytes(sampleBytes);

        return byteBuf;
    }
}