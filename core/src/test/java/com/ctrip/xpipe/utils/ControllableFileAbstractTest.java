package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 16, 2018
 */
public class ControllableFileAbstractTest extends AbstractTest {

    private File file;

    @Before
    public void beforeControllableFileAbstractTest() {

        file = new File(getTestFileDir(), getTestName() + ".log");
    }

    @Test
    public void testFileNotExist() throws IOException {

        int length = 1 << 10;
        try(FileOutputStream ous = new FileOutputStream(file)){
            ous.write(randomString(length).getBytes());
        }


        AbstractControllableFile controllableFile = new AbstractControllableFile(file) {
        };

        Assert.assertEquals(length, controllableFile.size());

        String testFileDir = getTestFileDir();

        logger.info("{}", testFileDir);
        FileUtils.recursiveDelete(new File(testFileDir));
        Assert.assertTrue(!file.exists());

        AbstractControllableFile controllableFileNew = new AbstractControllableFile(file) {
        };
        try{
            controllableFileNew.size();
            Assert.fail();
        }catch (Exception e){

        }

    }

    @Test
    public void testInterupt() throws IOException {

        Thread.currentThread().interrupt();

        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");

        try {
            randomAccessFile.getChannel().size();
            Assert.fail();
        } catch (ClosedByInterruptException e) {
            //expected
        }

        file.length();

        //clear interrupt
        Thread.interrupted();
    }

    @Test
    public void testEquals() throws FileNotFoundException, ExecutionException, InterruptedException {

        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        FileChannel channel = randomAccessFile.getChannel();
        byte[] data = randomString(randomInt(0, 1000)).getBytes();

        AtomicLong total = new AtomicLong(0);

        int readCount = 10;
        Semaphore write = new Semaphore(readCount);
        Semaphore read = new Semaphore(0);

        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {

                while (!Thread.currentThread().isInterrupted()) {

                    write.acquire(readCount);

                    byte[] data = randomString(randomInt(0, 1000)).getBytes();
                    total.addAndGet(data.length);
                    channel.write(ByteBuffer.wrap(data));

                    read.release(readCount);
                }
            }
        });


        SettableFuture<Boolean> success = SettableFuture.create();

        for (int i = 0; i < readCount / 2; i++) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {

                    while (!Thread.currentThread().isInterrupted()) {
                        read.acquire();
                        if (total.get() != file.length()) {
                            logger.info("[length not equals]");
                            success.set(false);
                        }
                        write.release();
                    }
                }
            });
        }

        for (int i = readCount / 2; i < readCount; i++) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {

                    while (!Thread.currentThread().isInterrupted()) {
                        read.acquire();

                        if (total.get() != channel.size()) {
                            logger.info("[channel not equals]");
                            success.set(false);
                        }

                        write.release();
                    }
                }
            });
        }

        try {
            success.get(1, TimeUnit.SECONDS);
            Assert.fail();
        } catch (TimeoutException e) {

        }

        logger.info("{}", file.length());
    }


    @Test
    public void testSizeAndFileLength() throws IOException {

        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        FileChannel channel = randomAccessFile.getChannel();
        byte[] data = randomString(randomInt(0, 1000)).getBytes();

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                channel.write(ByteBuffer.wrap(data));
            }
        }, 0, 1, TimeUnit.MILLISECONDS);


        int testCount = 1 << 15;
        long begin = System.currentTimeMillis();

        Long size = 0L;
        for (int i = 0; i < testCount; i++) {
//            size = channel.size();
            size = file.length();
        }
        long end = System.currentTimeMillis();

        logger.info("each {} ns", (end - begin) * 1000000 / testCount);
        logger.info("{}", size);

    }
}
