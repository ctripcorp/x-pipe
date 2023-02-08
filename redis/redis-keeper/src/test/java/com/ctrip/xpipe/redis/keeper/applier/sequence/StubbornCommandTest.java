package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.keeper.applier.command.StubbornCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.mocks.TestSupplierCommand;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Slight
 * <p>
 * Feb 07, 2022 10:06 PM
 */
public class StubbornCommandTest {

    int i = 0;

    Command<Integer> failTwice = new TestSupplierCommand<>(() -> {
        if (i < 2) {
            i++;
            return null;
        }
        return i;
    });

    int j = 0;

    Command<Integer> fail5Times = new TestSupplierCommand<>(() -> {
        if (j < 5) {
            j++;
            return null;
        }
        return j;
    });

    ScheduledExecutorService scheduledExecutorService;

    @Before
    public void setUp() throws Exception {
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
    }

    @After
    public void tearDown() throws Exception {
        scheduledExecutorService.shutdownNow();
    }

    @Test(expected = ExecutionException.class)
    public void failTwice() throws ExecutionException, InterruptedException {

        try {
            failTwice.execute().get();
        } catch (Throwable t) {
            assertEquals("no result", t.getCause().getMessage());
            throw t;
        }
    }

    @Test
    public void stubbornFail5Times() throws ExecutionException, InterruptedException {
        Integer result = new StubbornCommand<>(fail5Times, scheduledExecutorService, 3).execute().get();
        assertNull(result);
    }

    @Test
    public void stubbornFailTwice() throws ExecutionException, InterruptedException {
        Integer result = new StubbornCommand<>(failTwice, scheduledExecutorService).execute().get();
        assertEquals((Integer) 2, result);
    }
}