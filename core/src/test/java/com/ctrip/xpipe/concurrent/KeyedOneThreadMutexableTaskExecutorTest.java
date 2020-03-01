package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.command.TestCommand;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chen.zhu
 * <p>
 * Jan 22, 2020
 */
public class KeyedOneThreadMutexableTaskExecutorTest extends AbstractTest {

    private int sleepInterval = 100;
    private KeyedOneThreadMutexableTaskExecutor<String> keyed;

    @Before
    public void beforeKeyedOneThreadMutexableTaskExecutorTest(){
        keyed = new KeyedOneThreadMutexableTaskExecutor<>(executors, scheduled);
    }

    @Test
    public void testClearAndExecute() {
        int taskNum = 100;
        AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < taskNum; i++) {
            keyed.clearAndExecute("key" + i, new CountingCommand(counter, 10));
        }
        sleep(200);
        Assert.assertEquals(taskNum, counter.get());
    }

    @Test
    public void testSameKey(){

        BlockingCommand command1 =  new BlockingCommand(sleepInterval);
        BlockingCommand command2 =  new BlockingCommand(sleepInterval);

        keyed.execute("key1", command1);
        keyed.execute("key1", command2);

        sleep(sleepInterval/2);

        Assert.assertTrue(command1.isProcessing());
        Assert.assertFalse(command2.isProcessing());

    }

    @Test
    public void testDifferentKey(){

        BlockingCommand command1 =  new BlockingCommand(sleepInterval);
        BlockingCommand command2 =  new BlockingCommand(sleepInterval);

        keyed.execute("key1", command1);
        keyed.execute("key2", command2);

        sleep(sleepInterval/2);

        Assert.assertTrue(command1.isProcessing());
        Assert.assertTrue(command2.isProcessing());

    }

    @After
    public void afterKeyedOneThreadTaskExecutorTest() throws Exception{
        keyed.destroy();
    }

}