package com.ctrip.xpipe.payload;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.testutils.MemoryPrinter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public class DirectByteBufInStringOutPayloadTest extends AbstractTest {

    private DirectByteBufInStringOutPayload payload = new DirectByteBufInStringOutPayload();

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
                                DirectByteBufInStringOutPayload payload = new DirectByteBufInStringOutPayload();
                                payload.startInput();
                                payload.in(byteBuf);
                                payload.endInput();
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
        DirectByteBufInStringOutPayload payload = new DirectByteBufInStringOutPayload();
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
        DirectByteBufInStringOutPayload payload = new DirectByteBufInStringOutPayload();
        StringBuilder randomStr = new StringBuilder();
        payload.startInput();
        ByteBufAllocator allocator = new PooledByteBufAllocator();
        for(int i = 0; i < 100; i ++) {
            String delta = randomString(100);
            ByteBuf byteBuf = allocator.directBuffer(delta.length());
            byteBuf.writeBytes(delta.getBytes());
            randomStr.append(delta);
            payload.in(byteBuf);
//            byteBuf.release();
        }
        payload.endInput();
        Assert.assertEquals(randomStr.toString(), payload.toString());
        sleep(5000);
    }

    @Test
    public void testContinuouslyWithLargeInput() throws Exception {
        DirectByteBufInStringOutPayload payload = new DirectByteBufInStringOutPayload();
        StringBuilder randomStr = new StringBuilder();
        payload.startInput();

        for(int i = 0; i < 10; i ++) {
            String delta = randomString(1024);
            ByteBuf byteBuf = directByteBuf(delta.length());
            byteBuf.writeBytes(delta.getBytes());
            randomStr.append(delta);
            payload.in(byteBuf);
            byteBuf.release();
        }
        payload.endInput();
        Assert.assertEquals(randomStr.toString(), payload.toString());
        sleep(5000);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDoOut() throws IOException {
        String content = randomString();
        DirectByteBufInStringOutPayload payload = new DirectByteBufInStringOutPayload();
        payload.startInput();
        try (ByteArrayWritableByteChannel channel = new ByteArrayWritableByteChannel()) {

            ByteBuffer byteBuffer = ByteBuffer.wrap(content.getBytes());
            payload.in(Unpooled.wrappedBuffer(byteBuffer));

            payload.out(channel);
            byte[] result = channel.getResult();
            Assert.assertEquals(content, new String(result));
        }
        payload.endInput();
    }

    @Test
    public void testSize() {
        int length = "10.25.59.121,22399,5646210298ed600330ed2721941ebe7d563385a6,499377,FlightTicketReimbursementCacheGroup1+SHAOY,10.25.168.222,6379,0".getBytes().length;
        logger.info("{}", length);
    }
}