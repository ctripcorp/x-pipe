package com.ctrip.xpipe.utils.log;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestOperations;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.spring.RestTemplateFactory;

/**
 * @author wenchao.meng
 *         <p>
 *         Nov 1, 2016
 */
public class LogTest extends AbstractTest {

    @Test
    public void testAsyncLog() throws InterruptedException {

        String random = randomString(1024);
        int count = 20000;

        for (int i = 0; i < count; i++) {
            logger.info(random);
        }


        TimeUnit.SECONDS.sleep(10);
    }

    @Test
    public void testLevel() {
        logger.info("info");
        logger.debug("debug");
        logger.trace("trace");

    }

    @Test
    public void testLog() throws IOException {

        logger.error("{}", "nihao");
        logger.error("[testLog]", new IOException("io exception"));
        logger.error("[testLog]", new HttpServerErrorException(HttpStatus.BAD_GATEWAY, "statusCode",
                "responseBodyExample".getBytes(), Charset.defaultCharset()));

        logger.error("[testLog]", new Exception("simple exception"));
    }

    @SuppressWarnings("resource")
    @Test
    public void testFileIo() {

        try {
            RandomAccessFile writeFile = new RandomAccessFile("/opt/logs/test.log", "rw");
            FileChannel channel = writeFile.getChannel();
            channel.close();
            channel.size();
        } catch (Exception e) {
            logger.error("[testLog]", e);
        }
    }

    @Test
    public void testRest() {

        try {
            RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate();
            restOperations.delete(String.format("http://localhost:%d", randomPort()));
        } catch (Exception e) {
            logger.error("[testLog]", e);
        }
    }

    @Test
    public void testLogJunit() {

        logger.error("[testLogJunit]", new Exception());
    }

}
