package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.redis.console.notifier.EventType;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEvent;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEventListener4Sentinel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;

/**
 * @author chen.zhu
 *         <p>
 *         Feb 11, 2018
 */
public class DefaultClusterDeleteEventListenerTest {

    private DefaultClusterDeleteEventListener listener = new DefaultClusterDeleteEventListener();

    @Spy
    private ShardDeleteEventListener4Sentinel shardDeleteEventListener;

    @Spy
    private ShardDeleteEvent shardDeleteEvent = new ShardDeleteEvent("cluster", "shard",
            Executors.newScheduledThreadPool(1));

    @Before
    public void beforeClusterDeleteEventListenerTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void update() throws Exception {
        ClusterDeleteEvent clusterDeleteEvent = new ClusterDeleteEvent("cluster", 0,
                Executors.newScheduledThreadPool(1));

        shardDeleteEvent.addObserver(shardDeleteEventListener);

        clusterDeleteEvent.addShardEvent(shardDeleteEvent);

        listener.update(EventType.DELETE, clusterDeleteEvent);

        Mockito.verify(shardDeleteEvent).onEvent();

        Mockito.verify(shardDeleteEventListener).update(any(), any());
    }

}