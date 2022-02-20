package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.redis.keeper.applier.command.SequenceCommand;
import org.assertj.core.util.Lists;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/**
 * @author Slight
 * <p>
 * Jan 31, 2022 5:52 PM
 */
public class SequenceCommandTest {

    Executor stateThread = Executors.newSingleThreadExecutor();
    Executor workerThreads = Executors.newFixedThreadPool(100);

    @Test
    public void single() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);

        stateThread.execute(()->{

            final long startId = Thread.currentThread().getId();

            AbstractThreadSwitchCommand<String> command = new TestSleepCommand(100);

            SequenceCommand<String> sc = new SequenceCommand<>(command, stateThread, workerThreads);

            sc.execute().addListener((f)->{
                assertEquals(startId, Thread.currentThread().getId());
                latch.countDown();
            });
        });

        latch.await();
    }

    @Test
    public void waitFor300And500() throws ExecutionException, InterruptedException {

        SequenceCommand<String> sc300 = new SequenceCommand<>(new TestSleepCommand(300), stateThread, workerThreads);
        SequenceCommand<String> sc500 = new SequenceCommand<>(new TestSleepCommand(500), stateThread, workerThreads);

        sc300.execute();
        sc500.execute();

        SequenceCommand<String> wait = new SequenceCommand<>(Lists.newArrayList(sc300, sc500), new TestSleepCommand(200), stateThread, workerThreads);

        wait.execute().get();

    }
}