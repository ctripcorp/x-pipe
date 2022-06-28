package com.ctrip.xpipe.command;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 16, 2018
 */
public class DefaultRetryCommandFactoryTest extends AbstractTest {

    private RetryCommandFactory<String> retryCommandFactory;
    private int sleepMilli = 50;

    @Test
    public void testNoretry() throws InterruptedException, ExecutionException, TimeoutException {

        String msg = randomString();
        retryCommandFactory = DefaultRetryCommandFactory.noRetryFactory();

        Command<String> retryCommand = retryCommandFactory.createRetryCommand(new TestCommand(new Exception(msg), sleepMilli));

        try {
            retryCommand.execute().get(sleepMilli * 4, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (ExecutionException e) {
            Assert.assertEquals(msg, e.getCause().getMessage());
        }
    }

    @Test
    public void testDestroy() throws Exception {

        String msg = randomString();
        SettableFuture<Boolean> future = SettableFuture.create();
        retryCommandFactory = DefaultRetryCommandFactory.retryForever(scheduled, 1);
        Command<String> retryCommand = retryCommandFactory.createRetryCommand(new TestCommand(new Exception(msg), sleepMilli));
        retryCommand.execute(executors).addListener(new CommandFutureListener<String>() {
            @Override
            public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                logger.info("[operationComplete]");
                future.set(true);
            }
        });

        logger.info("[destroy retryCommandFactory]");
        retryCommandFactory.destroy();
        Assert.assertTrue(future.get(200, TimeUnit.MILLISECONDS));

    }
}
