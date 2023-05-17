package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpDataCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.mocks.TestLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.sequence.mocks.TestMSetCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.mocks.TestMultiDataCommandWrapper;
import com.ctrip.xpipe.redis.keeper.applier.sequence.mocks.TestSetCommand;
import com.ctrip.xpipe.utils.ClusterShardAwareThreadFactory;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author Slight
 * <p>
 * Feb 20, 2022 6:40 PM
 */
public class DefaultSequenceControllerTest extends AbstractTest {

    DefaultSequenceController controller = new DefaultSequenceController();

    @Before
    public void setUp() throws Exception {
        controller.stateThread = Executors.newScheduledThreadPool(1,
                ClusterShardAwareThreadFactory.create("test-cluster", "test-shard", "state-test-thread"));
        controller.workerThreads = Executors.newScheduledThreadPool(8,
                ClusterShardAwareThreadFactory.create("test-cluster", "test-shard", "worker-test-thread"));
        controller.scheduled = scheduled;
        controller.offsetRecorder = new AtomicLong(0);
        controller.initialize();
    }

    @After
    public void tearDown() throws Exception {
        controller.dispose();
    }

    @Test
    public void twoCommandsOnSameKey() throws ExecutionException, InterruptedException {

        TestSetCommand first = new TestSetCommand(100, "SET", "Key", "V1");
        TestSetCommand second = new TestSetCommand(200, "SET", "Key", "V2");

        assertEquals(first.key(), second.key());

        controller.submit(first, 0);
        controller.submit(second, 0);

        first.future().get();
        second.future().get();

        assertTrue(second.startTime >= first.endTime);
    }

    @Test
    public void multiKeyCommand() throws ExecutionException, InterruptedException, TimeoutException {

        TestLwmManager lwmManager = new TestLwmManager();
        controller.lwmManager = lwmManager;

        RedisOpDataCommand<String> command = spy(new TestMSetCommand(0L, "MSET", "A", "A", "B", "B"));

        TestMSetCommand first = new TestMSetCommand(100, "MSET", "A", "A");
        TestMSetCommand second = new TestMSetCommand(200, "MSET", "B", "B");

        when(command.gtid()).thenReturn("A:1");

        controller.submit(new TestMultiDataCommandWrapper(command, executors, Lists.newArrayList(first, second)), 0);

        first.future().get();
        second.future().get();

        waitConditionUntilTimeOut(()->lwmManager.lastSubmitTime >= first.endTime);
        waitConditionUntilTimeOut(()->lwmManager.lastSubmitTime >= second.endTime);

        assertEquals(1, lwmManager.count);
    }
}