package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 20, 2017
 */
public class DefaultControllableFileTest extends AbstractTest {

    private File file;
    private DefaultControllableFile controllableFile;

    @Before
    public void beforeDefaultControllableFileTest() throws IOException {
        file = new File(getTestFileDir() + "/" + getTestName());
        controllableFile = new DefaultControllableFile(file);

    }

    @Test
    public void testInterruptSizeNoRetry() throws IOException{

        AtomicInteger count = new AtomicInteger();
        ControllableFile controllableFile = new DefaultControllableFile(file){
            @Override
            public long size() {
                count.incrementAndGet();
                return super.size();
            }
        };

        Thread.currentThread().interrupt();
        try {
            controllableFile.size();
        }catch (Exception e){
            //ignore
        }

        Thread.interrupted();//clear interrupt

        Assert.assertEquals(1, count.get());
    }


        @Test
    public void testCloseSize() throws IOException{

        int dataLen = 1024;
        AtomicInteger count = new AtomicInteger();

        ControllableFile controllableFile = new DefaultControllableFile(file){

            @Override
            protected void doOpen() throws IOException {
                super.doOpen();
                int current = count.incrementAndGet();
                if(current == 2){
                    logger.info("[close in doOpen]");
                    getFileChannel().close();
                }
            }
        };

        controllableFile.getFileChannel().write(ByteBuffer.wrap(randomString(dataLen).getBytes()));
        Assert.assertEquals(dataLen, controllableFile.size());
        controllableFile.close();
        // file close can not reopen, will throw exception
        // Assert.assertEquals(dataLen, controllableFile.size());
    }



    @Test
    public void testInterrupt() {

        Thread.currentThread().interrupt();
        try {
            long size = controllableFile.size();
            logger.info("{}", size);
        } catch (Exception e) {
            logger.error("[testInterrupt]", e);
        }

        Thread.interrupted();//clear interrupt
    }


    @Test
    public void testConcurrentRead() throws IOException, InterruptedException {

        int concurrent = 10;

        List<FileChannel> channels = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(concurrent);

        for (int i = 0; i < concurrent; i++) {

            executors.execute(new AbstractExceptionLogTask() {

                @Override
                protected void doRun() throws Exception {
                    try {
                        FileChannel fileChannel = controllableFile.getFileChannel();
                        synchronized (channels) {
                            channels.add(fileChannel);
                        }
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();

        logger.info("{}", channels.size());
        Assert.assertEquals(concurrent, channels.size());
        for (int i = 1; i < channels.size(); i++) {
            Assert.assertEquals(channels.get(0), channels.get(i));
        }

    }


    @After
    public void afterDefaultControllableFileTest() throws IOException {
        controllableFile.close();
    }
}
