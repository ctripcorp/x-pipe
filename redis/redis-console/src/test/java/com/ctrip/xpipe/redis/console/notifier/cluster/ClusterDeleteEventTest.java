package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEvent;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEventListener4Sentinel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.concurrent.Executors;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */
public class ClusterDeleteEventTest {

    @Spy
    private ShardDeleteEvent shardDeleteEvent;

    @Before
    public void beforeClusterDeleteEventTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testOnEvent() throws InterruptedException {
        ClusterDeleteEvent clusterDeleteEvent = new ClusterDeleteEvent("cluster", Executors.newScheduledThreadPool(1));
        shardDeleteEvent.setClusterName("cluster").addObserver(new ShardDeleteEventListener4Sentinel());
        clusterDeleteEvent.addShardEvent(shardDeleteEvent);

        clusterDeleteEvent.addObserver(new DefaultClusterDeleteEventListener());
        clusterDeleteEvent.onEvent();

        Mockito.verify(shardDeleteEvent).onEvent();
    }
}