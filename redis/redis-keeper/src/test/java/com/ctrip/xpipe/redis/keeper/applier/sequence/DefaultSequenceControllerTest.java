package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.keeper.applier.sequence.mocks.TestSetCommand;
import com.ctrip.xpipe.utils.ClusterShardAwareThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Slight
 * <p>
 * Feb 20, 2022 6:40 PM
 */
public class DefaultSequenceControllerTest extends AbstractTest {

    DefaultSequenceController controller;

    @Before
    public void setUp() throws Exception {
        controller = new DefaultSequenceController();
        controller.stateThread = Executors.newScheduledThreadPool(1,
                ClusterShardAwareThreadFactory.create("test-cluster", "test-shard", "state-test-thread"));
        controller.workerThreads = Executors.newScheduledThreadPool(8,
                ClusterShardAwareThreadFactory.create("test-cluster", "test-shard", "worker-test-thread"));
        controller.scheduled = scheduled;
        controller.offsetRecorder = new AtomicLong(0);
        controller.applierStatisticRef = new AtomicReference<>();
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

}