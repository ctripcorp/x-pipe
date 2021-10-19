package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.exception.CommandNotExecuteException;
import com.ctrip.xpipe.netty.commands.AbstractNettyRequestResponseCommand;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.verification.AtLeast;

import java.util.List;
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
    private RequestResponseCommand<Void> command;

    @Before
    public void beforeMutexableOneThreadTaskExecutorTest() {
        MockitoAnnotations.initMocks(this);
        ExecutorService executorService = Executors.newCachedThreadPool();
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
        oneThreadTaskExecutor = new MutexableOneThreadTaskExecutor(executorService, scheduledExecutorService);
    }

    @After
    public void afterMutexableOneThreadTaskExecutorTest() {
        executors.shutdownNow();
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

    @Test
    public void testAcceptCommandAfterClearAndExecute() throws Exception {
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
        waitConditionUntilTimeOut(()->oneThreadTaskExecutor.tasks.isEmpty(), 20000);
        sleep(50);
        Assert.assertEquals(1 + taskNum, counter.get());
    }

    @Test
    public void testCommandHangWontImpact() throws TimeoutException {
        BlockingCommand blockingCommand = new BlockingCommand(100).setTimeout(200);
        CommandFuture future = blockingCommand.future();
        oneThreadTaskExecutor.executeCommand(blockingCommand);
        sleep(5);
        oneThreadTaskExecutor.clearAndExecuteCommand(new CountingCommand(new AtomicInteger(), 1));
        sleep(20);
        waitConditionUntilTimeOut(()->oneThreadTaskExecutor.tasks.isEmpty(), 1500);
        Assert.assertTrue(future.isSuccess());
    }

    @Test
    public void testLongTermRunningTaskCanceled() throws Exception {
        BlockingCommand longTermTask = new BlockingCommand(2000).setTimeout(200);
        CommandFuture future = longTermTask.future();
        oneThreadTaskExecutor.executeCommand(longTermTask);
        sleep(5);
        CountingCommand shouldExecute = new CountingCommand(new AtomicInteger(), 1);
        oneThreadTaskExecutor.clearAndExecuteCommand(shouldExecute);
        sleep(300);
        waitConditionUntilTimeOut(()->oneThreadTaskExecutor.tasks.isEmpty(), 3000);
        Assert.assertFalse(future.isSuccess());
        Assert.assertTrue(future.cause() instanceof CommandTimeoutException);
        Assert.assertTrue(shouldExecute.future().isSuccess());
    }

    @Test
    public void testFinalConsistency() throws TimeoutException {
        AtomicReference<String> state = new AtomicReference<>();
        AtomicInteger taskDone = new AtomicInteger();
        int taskNum = 100;
        CyclicBarrier barrier = new CyclicBarrier(taskNum + 1);
        for (int i = 0; i < taskNum; i++) {
            int finalI = i;
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
                    StateCommand command = new StateCommand("not-expected", state, finalI);
                    command.future().addListener(commandFuture -> {
                        taskDone.incrementAndGet();});
                    try {
                        oneThreadTaskExecutor.executeCommand(command);
                    } catch (IllegalStateException e) {
                        taskDone.incrementAndGet();
                    }
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
                sleep(30);
                oneThreadTaskExecutor.clearAndExecuteCommand(new StateCommand("expected", state, 1));
                oneThreadTaskExecutor.clearAndExecuteCommand(new StateCommand("expected", state, 1));
            }
        });
        waitConditionUntilTimeOut(()->oneThreadTaskExecutor.tasks.isEmpty(), 10000);
        waitConditionUntilTimeOut(()->taskDone.get() == taskNum, 10000);
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            Assert.assertTrue(oneThreadTaskExecutor.tasks.isEmpty());
            Assert.assertEquals("expected", state.get());
        }));
        logger.info("[task done]{}", taskDone);
    }

    @Test
    public void durableCall() throws TimeoutException, InterruptedException {
        AtomicReference<String> state = new AtomicReference<>();
        AtomicInteger taskDone = new AtomicInteger();
        AtomicInteger counter = new AtomicInteger();
        int taskNum = 100;
        CyclicBarrier barrier = new CyclicBarrier(taskNum + 1);
        for (int i = 0; i < taskNum; i++) {
            int finalI = i;
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
                    StateCommand command = new StateCommand("not-expected", state, finalI);
                    command.future().addListener(commandFuture -> {
                        taskDone.incrementAndGet();});
                    try {
                        oneThreadTaskExecutor.executeCommand(command);
                    } catch (IllegalStateException e) {
                        taskDone.incrementAndGet();
                    }
                }
            });
        }

        CountDownLatch latch = new CountDownLatch(1);
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
                oneThreadTaskExecutor.clearAndExecuteCommand(new StateCommand("expected", state, 1));
                latch.countDown();
            }
        });
        latch.await();
        sleep(50);
        oneThreadTaskExecutor.clearAndExecuteCommand(new StateCommand("expected-2", state, 2));
        sleep(200);
        waitConditionUntilTimeOut(()->oneThreadTaskExecutor.tasks.isEmpty(), 1000);
        waitConditionUntilTimeOut(()->taskDone.get() == taskNum, 1000);
        Assert.assertTrue(oneThreadTaskExecutor.tasks.isEmpty());
        Assert.assertEquals("expected-2", state.get());
        logger.info("[task done]{}", taskDone);
    }

    @Test
    public void durableCall2() throws TimeoutException, InterruptedException {
        AtomicReference<String> state = new AtomicReference<>();
        AtomicInteger taskDone = new AtomicInteger();
        AtomicInteger counter = new AtomicInteger();
        int taskNum = 100;
        CyclicBarrier barrier = new CyclicBarrier(taskNum);
        CountDownLatch latch = new CountDownLatch(taskNum);
        List<CommandFuture> futures = Lists.newCopyOnWriteArrayList();
        for (int i = 0; i < taskNum; i++) {
            int finalI = i;
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

                    StateCommand command = new StateCommand("expected-" + finalI, state, finalI);
                    futures.add(command.future());
                    command.future().addListener(commandFuture -> {
//                        logger.info("[task done] {}", command.expected);
                        taskDone.incrementAndGet();
                    });

                    oneThreadTaskExecutor.clearAndExecuteCommand(command);

                    latch.countDown();

                }
            });
        }

        latch.await();
        sleep(300);
        oneThreadTaskExecutor.clearAndExecuteCommand(new StateCommand("expected", state, 12345));
        sleep(200);
        waitConditionUntilTimeOut(()->oneThreadTaskExecutor.tasks.isEmpty(), 1000);
        try {
            waitConditionUntilTimeOut(()->taskDone.get() >= taskNum, 10000);
        } catch (Exception ignore) {
            for (CommandFuture future : futures) {
                if (!future.isDone()) {
                    logger.error("[NOT DONE] {}", future.command().getName());
                }
            }
            logger.error("[taskDone] {}", taskDone, ignore);
            throw ignore;
        }

        Assert.assertTrue(oneThreadTaskExecutor.tasks.isEmpty());
        Assert.assertEquals("expected", state.get());
        logger.info("[task done]{}", taskDone);
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
            oneThreadTaskExecutor.executeCommand(new AbstractRequestResponseCommand<Object>() {

                @Override
                public int getCommandTimeoutMilli() {
                    return 10;
                }

                @Override
                protected void doExecute()  {
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

    public static class StateCommand extends AbstractCommand<Void> implements RequestResponseCommand<Void> {

        private String expected;

        private AtomicReference<String> state;

        private int counter;

        public StateCommand(String expected, AtomicReference<String> state, int counter) {
            this.expected = expected;
            this.state = state;
            this.counter = counter;
        }

        @Override
        protected void doExecute() throws Exception {
            if(future().isDone()) {
                return;
            }
            state.set(expected);
            future().setSuccess();
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName() + counter;
        }

        @Override
        public int getCommandTimeoutMilli() {
            return 10;
        }
    }

    public static abstract class AbstractRequestResponseCommand<V> extends AbstractCommand<V> implements RequestResponseCommand<V> {

    }
}