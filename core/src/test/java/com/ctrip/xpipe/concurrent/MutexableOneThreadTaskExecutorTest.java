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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.verification.AtLeast;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Jan 22, 2020
 */
public class MutexableOneThreadTaskExecutorTest extends AbstractTest {

    private MutexableOneThreadTaskExecutor oneThreadTaskExecutor;

    @Mock
    private Command<Void> command;

    @Before
    public void beforeMutexableOneThreadTaskExecutorTest() {
        MockitoAnnotations.initMocks(this);
        oneThreadTaskExecutor = new MutexableOneThreadTaskExecutor(executors);
    }

    @Test
    public void testExecuteMutexableCommand() throws TimeoutException {
        AtomicReference<Throwable> exception = new AtomicReference<>();
        Command command = new BlockingCommand(10 * 1000);
        command.future().addListener(commandFuture -> {
            if (!commandFuture.isSuccess()) {
                exception.set(commandFuture.cause());
            }
        });
        oneThreadTaskExecutor.executeCommand(command);
        int taskNum = 100;
        AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < taskNum; i++) {
            oneThreadTaskExecutor.executeCommand(new CountingCommand(counter, 10));
        }
        oneThreadTaskExecutor.clearAndExecuteCommand(new CountingCommand(counter, 1));
        waitConditionUntilTimeOut(()->oneThreadTaskExecutor.tasks.isEmpty(), 2000);
        sleep(50);
        Assert.assertEquals(1, counter.get());
    }

    @Test
    public void testClearAndExecuteThenExecute() throws TimeoutException {
        Command command = new BlockingCommand(10 * 1000);
        oneThreadTaskExecutor.executeCommand(command);
        int taskNum = 100;
        AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < taskNum; i++) {
            oneThreadTaskExecutor.executeCommand(new CountingCommand(counter, 10));
        }
        oneThreadTaskExecutor.clearAndExecuteCommand(new CountingCommand(counter, 1));
        sleep(100);
        for (int i = 0; i < taskNum; i++) {
            oneThreadTaskExecutor.executeCommand(new CountingCommand(counter, 10));
        }
        waitConditionUntilTimeOut(()->oneThreadTaskExecutor.tasks.isEmpty(), 2000);
        sleep(50);
        Assert.assertEquals(1 + taskNum, counter.get());
    }

    @Test(expected = IllegalStateException.class)
    public void testNotAcceptCommandDuringBlocking() throws Exception {
        Command command = new BlockingCommand(10 * 1000);
        oneThreadTaskExecutor.executeCommand(command);
        int taskNum = 100;
        AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < taskNum; i++) {
            oneThreadTaskExecutor.executeCommand(new CountingCommand(counter, 10));
        }
        oneThreadTaskExecutor.clearAndExecuteCommand(new CountingCommand(counter, 1));
        for (int i = 0; i < taskNum; i++) {
            oneThreadTaskExecutor.executeCommand(new CountingCommand(counter, 10));
        }
        waitConditionUntilTimeOut(()->oneThreadTaskExecutor.tasks.isEmpty(), 2000);
        sleep(50);
        Assert.assertTrue(1 + taskNum > counter.get());
    }

    @Test
    public void testFinalConsistency() throws TimeoutException {
        AtomicReference<String> state = new AtomicReference<>();
        AtomicInteger taskDone = new AtomicInteger();
        AtomicInteger counter = new AtomicInteger();
        int taskNum = 100;
        CyclicBarrier barrier = new CyclicBarrier(taskNum + 1);
        for (int i = 0; i < taskNum; i++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                    StateCommand command = new StateCommand("not-expected", state, counter);
                    command.future().addListener(commandFuture -> {taskDone.incrementAndGet();});
                    oneThreadTaskExecutor.executeCommand(command);
                }
            });
        }
        executors.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
                sleep(10);
                oneThreadTaskExecutor.clearAndExecuteCommand(new StateCommand("expected", state, counter));
            }
        });
        sleep(200);
        waitConditionUntilTimeOut(()->oneThreadTaskExecutor.tasks.isEmpty(), 1000);
        Assert.assertTrue(oneThreadTaskExecutor.tasks.isEmpty());
        Assert.assertEquals("expected", state.get());
        Assert.assertTrue(counter.get() > 1);
        logger.info("[task done]", taskDone);
    }

    @Test
    public void testStart() {
        when(command.future()).thenReturn(new DefaultCommandFuture<>());
        oneThreadTaskExecutor.executeCommand(command);
        sleep(50);
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

    public static class StateCommand extends AbstractCommand<Void> {

        private String expected;

        private AtomicReference<String> state;

        private AtomicInteger counter;

        public StateCommand(String expected, AtomicReference<String> state, AtomicInteger counter) {
            this.expected = expected;
            this.state = state;
            this.counter = counter;
        }

        @Override
        protected void doExecute() throws Exception {
            counter.incrementAndGet();
            state.set(expected);
            future().setSuccess();
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }
    }

}