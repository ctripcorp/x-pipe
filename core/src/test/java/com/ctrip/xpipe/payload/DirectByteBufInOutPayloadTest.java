package com.ctrip.xpipe.payload;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.testutils.MemoryPrinter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CountDownLatch;

public class DirectByteBufInOutPayloadTest extends AbstractTest {

    private DirectByteBufInOutPayload payload = new DirectByteBufInOutPayload();

    @Test
    public void testDoIn() throws IOException {
        String randomStr = randomString();

        ByteBuf byteBuf = directByteBuf(randomStr.length());

        byteBuf.writeBytes(randomStr.getBytes());
        payload.startInput();
        payload.in(byteBuf);
        payload.endInput();


        final ByteBuf result = directByteBuf(randomStr.length());

        payload.startOutput();
        long wroteLength = payload.out(new WritableByteChannel() {

            @Override
            public boolean isOpen() {
                return false;
            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public int write(ByteBuffer src) throws IOException {

                int readable = result.readableBytes();
                result.writeBytes(src);
                return result.readableBytes() - readable;
            }
        });
        payload.endOutput();


        Assert.assertEquals(randomStr.length(), wroteLength);

        byte []resultArray = new byte[(int) wroteLength];
        result.readBytes(resultArray);
        Assert.assertEquals(randomStr, new String(resultArray));
    }

    //manually check
    @Test
    public void testMemory() throws InterruptedException {
        final MemoryPrinter memoryPrinter = new MemoryPrinter(scheduled);

        memoryPrinter.printMemory();

        final int length = 1 << 10;
        int concurrentCount = 10;
        final CountDownLatch latch = new CountDownLatch(concurrentCount);

        final ByteBuf byteBuf = directByteBuf(length);

        byteBuf.writeBytes(randomString(length).getBytes());

        byte []dst = new byte[length];
        byteBuf.readBytes(dst);

        memoryPrinter.printMemory();

        for(int i=0;i<concurrentCount;i++){

            Thread current = new Thread(
                    new AbstractExceptionLogTask() {
                        @Override
                        protected void doRun() throws Exception {

                            try{
                                byteBuf.readerIndex(0);
                                DirectByteBufInOutPayload payload = new DirectByteBufInOutPayload();
                                payload.in(byteBuf);
                            }finally{
                                latch.countDown();
                            }
                        }
                    });
            current.start();
            memoryPrinter.printMemory();

        }

        latch.await();
    }

    @Test
    public void testScaleOut() throws Exception {
        DirectByteBufInOutPayload payload = new DirectByteBufInOutPayload();
        String randomStr = randomString();

        ByteBuf byteBuf = directByteBuf(randomStr.length());

        randomStr += randomString(2<<11);

        byteBuf.writeBytes(randomStr.getBytes());
        payload.startInput();
        payload.in(byteBuf);
        payload.endInput();
        Assert.assertEquals(randomStr, payload.toString());
    }

    @Test
    public void testContinuouslyInput() throws Exception {
        DirectByteBufInOutPayload payload = new DirectByteBufInOutPayload();
        StringBuilder randomStr = new StringBuilder();
        payload.startInput();

        for(int i = 0; i < 100; i ++) {
            String delta = randomString(100);
            ByteBuf byteBuf = directByteBuf(delta.length());
            byteBuf.writeBytes(delta.getBytes());
            randomStr.append(delta);
            payload.in(byteBuf);
        }
        payload.endInput();
        Assert.assertEquals(randomStr.toString(), payload.toString());
    }

    @Test
    public void testContinuouslyWithLargeInput() throws Exception {
        DirectByteBufInOutPayload payload = new DirectByteBufInOutPayload();
        StringBuilder randomStr = new StringBuilder();
        payload.startInput();

        for(int i = 0; i < 10; i ++) {
            String delta = randomString(1024);
            ByteBuf byteBuf = directByteBuf(delta.length());
            byteBuf.writeBytes(delta.getBytes());
            randomStr.append(delta);
            payload.in(byteBuf);
        }
        payload.endInput();
        Assert.assertEquals(randomStr.toString(), payload.toString());
    }

    @Test
    public void testDoOut() throws IOException {
        String content = randomString();
        DirectByteBufInOutPayload payload = new DirectByteBufInOutPayload();

        try (ByteArrayWritableByteChannel channel = new ByteArrayWritableByteChannel()) {

            ByteBuffer byteBuffer = ByteBuffer.wrap(content.getBytes());
            payload.in(Unpooled.wrappedBuffer(byteBuffer));

            payload.out(channel);
            byte[] result = channel.getResult();
            Assert.assertEquals(content, new String(result));
        }
    }
}