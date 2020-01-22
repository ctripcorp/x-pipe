package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Jan 22, 2020
 */
public class MutexableOneThreadTaskExecutorTest extends AbstractTest {

    private MutexableOneThreadTaskExecutor oneThreadTaskExecutor = new MutexableOneThreadTaskExecutor(executors);

    @Before
    public void beforeMutexableOneThreadTaskExecutorTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testExecuteCommand() {
    }

    @Test
    public void testExecuteMutexableCommand() {

    }
}