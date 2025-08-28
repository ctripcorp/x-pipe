package com.ctrip.xpipe.netty;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.channel.ConnectTimeoutException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 26, 2018
 */
public class TcpPortCheckCommandTest extends AbstractTest {

    private int count = 10;

    @Test
    public void testOk() throws Exception {

        Server server = startEchoServer();

        for (int i = 0; i < count; i++) {
            TcpPortCheckCommand checkCommand = new TcpPortCheckCommand("localhost", server.getPort());
            CommandFuture<Boolean> future = checkCommand.execute();
            Assert.assertTrue(future.get(500, TimeUnit.SECONDS));
        }


    }

    @Test
    public void testFail() throws InterruptedException, ExecutionException, TimeoutException {

        int port = randomPort();

        for (int i = 0; i < count; i++) {
            TcpPortCheckCommand checkCommand = new TcpPortCheckCommand("localhost", port);
            CommandFuture<Boolean> future = checkCommand.execute();
            try {
                future.get(500, TimeUnit.SECONDS);
                Assert.fail();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void testFail2() throws InterruptedException, ExecutionException, TimeoutException {

        int port = randomPort();

        long start = System.currentTimeMillis();

        TcpPortCheckCommand checkCommand = new TcpPortCheckCommand("10.0.0.1", port, 500);
        CommandFuture<Boolean> future = checkCommand.execute();
        try {
            future.get(5, TimeUnit.SECONDS);
            Assert.fail();
        } catch (Exception e) {
            long end = System.currentTimeMillis();
            Assert.assertTrue(end - start < 1000);
            Assert.assertTrue(e.getCause() instanceof ConnectTimeoutException);
        }

    }

//    @Test
    public void testResource() throws Exception {

        Server server = startEchoServer();
        int i = 0;
        try {
            for (; i < (1 << 10); i++) {
                TcpPortCheckCommand checkCommand = new TcpPortCheckCommand("localhost", server.getPort());
                CommandFuture<Boolean> future = checkCommand.execute();
                Assert.assertTrue(future.get(500, TimeUnit.SECONDS));
            }
        } catch (Exception e) {
            logger.error("[testResource]" + i, e);
        }


    }

    //    @Test
    public void testTimeout() throws IOException {

        TcpPortCheckCommand checkCommand = new TcpPortCheckCommand("10.2.24.217", 2181);

        logger.info("[begin]");
        checkCommand.execute();
        logger.info("[end]");

        waitForAnyKey();
    }

}
