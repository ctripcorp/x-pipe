package com.ctrip.xpipe.redis.keeper.applier.sequence;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/**
 * @author Slight
 * <p>
 * Jan 31, 2022 5:52 PM
 */
public class SequenceCommandTest {

    Executor executor = Executors.newSingleThreadExecutor();

    @Test
    public void single() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);

        executor.execute(()->{

            final long startId = Thread.currentThread().getId();

            ThreadSwitchCommand command = new ThreadSwitchCommand(100);

            SequenceCommand<String> sc = new SequenceCommand<>(command, executor);

            sc.execute().addListener((f)->{
                assertEquals(startId, Thread.currentThread().getId());
                latch.countDown();
            });
        });

        latch.await();
    }
}