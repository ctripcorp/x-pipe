package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.redis.keeper.applier.command.SequenceCommand;
import org.assertj.core.util.Lists;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void waitFor100And300() throws ExecutionException, InterruptedException {

        long start = System.currentTimeMillis();

        TestSleepCommand tsc100 = new TestSleepCommand(100);
        TestSleepCommand tsc300 = new TestSleepCommand(300);
        TestSleepCommand tsc200 = new TestSleepCommand(200);

        SequenceCommand<String> sc300 = new SequenceCommand<>(tsc100, stateThread, workerThreads);
        SequenceCommand<String> sc500 = new SequenceCommand<>(tsc300, stateThread, workerThreads);

        sc300.execute();
        sc500.execute();

        SequenceCommand<String> wait = new SequenceCommand<>(Lists.newArrayList(sc300, sc500), tsc200, stateThread, workerThreads);

        wait.execute().get();

        assertTrue(System.currentTimeMillis() - start >= 700);
        assertTrue(tsc200.startTime >= tsc300.endTime && tsc200.startTime >= tsc100.endTime);
    }
}