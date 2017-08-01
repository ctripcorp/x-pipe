package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.command.AbstractCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.verification.AtLeast;

import static org.mockito.Mockito.*;

import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.retry.RetryTemplate;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.retry.RetryNTimes;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    public void testStart() {

        oneThreadTaskExecutor.executeCommand(command);
        sleep(20);
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
                    return getTestName();
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
    public void testStartMtimes() {

        int times = 50;
        CommandFuture<Void> future = new DefaultCommandFuture<>();
        when(command.execute()).thenReturn(future);
        future.setSuccess();
        for (int i = 0; i < times; i++) {
            oneThreadTaskExecutor.executeCommand(command);
            sleep(30);
            verify(command, times(i + 1)).execute();
        }
    }

    @Test
    public void testRetryTemplate() throws TimeoutException {

        int retryTimes = 10;
        CommandFuture<Void> future = new DefaultCommandFuture<>();
        when(command.execute()).thenReturn(future);
        future.setFailure(new Exception());

        RetryTemplate<Void> retryTemplate = new RetryNTimes<>(retryTimes);

        OneThreadTaskExecutor oneThreadTaskExecutor = new OneThreadTaskExecutor(retryTemplate, executors);

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

        RetryTemplate<Void> retryTemplate = RetryNTimes.retryForEver(new RetryDelay(10));

        OneThreadTaskExecutor oneThreadTaskExecutor = new OneThreadTaskExecutor(retryTemplate, executors);

        oneThreadTaskExecutor.executeCommand(command);
        sleep(100);

        oneThreadTaskExecutor.destroy();
        retryTemplate.destroy();
        sleep(10);
        verify(command, new AtLeast(1)).execute();
        verify(command, new AtLeast(1)).reset();

        sleep(100);
        verifyNoMoreInteractions(command);
    }


}
