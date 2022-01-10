package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.command.RetryCommandFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.verification.AtLeast;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Queue;
import java.util.concurrent.*;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 14, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class OneThreadTaskExecutorTest extends AbstractTest {

    @Mock
    private Command<Void> command;

    protected OneThreadTaskExecutor oneThreadTaskExecutor;

    @Before
    public void beforeOneThreadTaskExecutorTest() {
        oneThreadTaskExecutor = new OneThreadTaskExecutor(executors);
    }

    @Test
    public void testStart() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return new DefaultCommandFuture<>();
        }).when(command).execute();
        oneThreadTaskExecutor.executeCommand(command);

        Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
        verify(command).execute();
    }

    @Test
    public void testSequence() throws InterruptedException {

        int count = 100;
        Queue<Integer> list = new ConcurrentLinkedQueue<>();
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            int finalI = i;
            oneThreadTaskExecutor.executeCommand(new AbstractCommand<Object>() {

                @Override
                protected void doExecute() throws Exception {
                    try{
                        logger.debug("{}", this);
                        list.offer(finalI);
                        future().setSuccess();
                    }finally {
                        latch.countDown();
                    }
                }

                @Override
                protected void doReset() {

                }
                @Override
                public String getName() {
                    return getTestName() + ":" + finalI;
                }
            });
        }

        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(count, list.size());
        int previous = -1;
        while (true){
            Integer current = list.poll();
            if(current == null){
                break;
            }

            Assert.assertTrue(current > previous);
            previous = current;
        }
    }

    @Test
    public void testStartMtimes() throws Exception {
        int times = 50;
        CountDownLatch latch = new CountDownLatch(times);
        CommandFuture<Void> future = new DefaultCommandFuture<>();
        doAnswer(invocation -> {
            latch.countDown();
            return future;
        }).when(command).execute();
        future.setSuccess();
        for (int i = 0; i < times; i++) {
            oneThreadTaskExecutor.executeCommand(command);
        }

        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
        verify(command, times(times)).execute();
    }

    @Test
    public void testRetryTemplate() throws TimeoutException {

        int retryTimes = 10;
        CommandFuture<Void> future = new DefaultCommandFuture<>();
        when(command.execute()).thenReturn(future);
        future.setFailure(new Exception());

        RetryCommandFactory retryCommandFactory = DefaultRetryCommandFactory.retryNTimes(scheduled, retryTimes, 10);
        OneThreadTaskExecutor oneThreadTaskExecutor = new OneThreadTaskExecutor(retryCommandFactory, executors);

        oneThreadTaskExecutor.executeCommand(command);

        waitConditionUntilTimeOut(() -> {
            try {
                verify(command, times(retryTimes + 1)).execute();
                return true;
            } catch (Throwable e) {
            }
            return false;
        }, 2000);
    }

    @Test
    public void testClose() throws Exception {

        CommandFuture<Void> future = new DefaultCommandFuture<>();
        when(command.execute()).thenReturn(future);
        future.setFailure(new Exception());

        RetryCommandFactory<Void> retryCommandFactory = DefaultRetryCommandFactory.retryForever(scheduled, 0);

        OneThreadTaskExecutor oneThreadTaskExecutor = new OneThreadTaskExecutor(retryCommandFactory, executors);

        oneThreadTaskExecutor.executeCommand(command);
        sleep(100);

        logger.info("[testClose][destroy]");
        oneThreadTaskExecutor.destroy();
        retryCommandFactory.destroy();

        sleep(50);
        verify(command, new AtLeast(1)).execute();
        verify(command, new AtLeast(1)).reset();

        logger.info("[testClose][sleep verify no more interactions]");
        sleep(100);
        verifyNoMoreInteractions(command);
    }


}
