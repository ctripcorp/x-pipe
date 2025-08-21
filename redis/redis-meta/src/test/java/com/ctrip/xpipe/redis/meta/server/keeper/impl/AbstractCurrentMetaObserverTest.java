package com.ctrip.xpipe.redis.meta.server.keeper.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;

/**
 * @author chen.zhu
 *         <p>
 *         Sep 28, 2020
 */
public class AbstractCurrentMetaObserverTest extends AbstractTest {

    private AbstractCurrentMetaObserver observer;

    @Mock
    protected CurrentMetaManager currentMetaManager;

    @Mock
    protected CurrentClusterServer currentClusterServer;

    @Before
    public void beforeAbstractCurrentMetaObserverTest() {
        MockitoAnnotations.initMocks(this);
        observer = new AbstractCurrentMetaObserver() {

            @Override
            public Set<ClusterType> getSupportClusterTypes() {
                return Sets.newHashSet(ClusterType.ONE_WAY);
            }

            @Override
            protected void handleClusterModified(ClusterMetaComparator comparator) {

            }

            @Override
            protected void handleClusterDeleted(ClusterMeta clusterMeta) {

            }

            @Override
            protected void handleClusterAdd(ClusterMeta clusterMeta) {

            }
        };
        observer.setCurrentMetaManager(currentMetaManager);
    }

    @Test
    public void testRegisterJob() throws Exception {
        doThrow(new IllegalArgumentException("No such cluster/shard")).when(currentMetaManager)
                .addResource(anyLong(), anyLong(), any(Releasable.class));
        AtomicInteger innerCounter = new AtomicInteger();
        ReleasableCounter counter = new ReleasableCounter(innerCounter);
        counter.start();
        observer.registerJob(1L, 1L, counter);
        int waitingTasks = 5;
        sleep(waitingTasks * 10);
        Assert.assertTrue(innerCounter.get() < waitingTasks);
    }

    private class ReleasableCounter extends AbstractCommand<Void> implements Releasable, Startable {

        private AtomicBoolean started = new AtomicBoolean(false);

        private AtomicInteger counter;

        public ReleasableCounter(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public void release() throws Exception {
            if (started.compareAndSet(true, false)) {
                getLogger().info("[release]");
            }
        }

        @Override
        protected void doExecute() throws Exception {
            started.set(true);
            while (started.get() && !Thread.currentThread().isInterrupted()) {
                counter.incrementAndGet();
                sleep(10);
            }
        }

        @Override
        protected void doReset() {
            counter.set(0);
        }

        @Override
        public String getName() {
            return "ReleasableCounter";
        }

        @Override
        public void start() throws Exception {
            execute(executors);
        }
    }
}