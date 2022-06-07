package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.keeper.applier.command.StubbornCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.mocks.TestSupplierCommand;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/**
 * @author Slight
 * <p>
 * Feb 07, 2022 10:06 PM
 */
public class StubbornCommandTest {

    int i = 0;

    Command<Integer> failTwice = new TestSupplierCommand<>(()->{
        if (i < 2) {
            i ++;
            return null;
        }
        return i;
    });


    @Test (expected = ExecutionException.class)
    public void failTwice() throws ExecutionException, InterruptedException {

        try {
            failTwice.execute().get();
        } catch (Throwable t) {
            assertEquals("no result", t.getCause().getMessage());
            throw t;
        }
    }

    @Test
    public void stubbornFailTwice() throws ExecutionException, InterruptedException {
        int result = new StubbornCommand<>(failTwice).execute().get();
        assertEquals(2, result);;
    }
}