package com.ctrip.xpipe.redis.console.notifier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultClusterMonitorModifiedNotifierTest {

    @Mock
    private DrBeaconClusterMonitorNotifier drNotifier;

    @Mock
    private SentinelBeaconClusterMonitorNotifier sentinelNotifier;

    private DefaultClusterMonitorModifiedNotifier notifier;

    @Before
    public void setupDefaultClusterMonitorModifiedNotifierTest() {
        notifier = new DefaultClusterMonitorModifiedNotifier(drNotifier, sentinelNotifier);
    }

    @Test
    public void testNotifyClusterChange() {
        Mockito.when(drNotifier.needNotify("cluster1", null, 1L)).thenReturn(true);
        Mockito.when(sentinelNotifier.needNotify("cluster1", null, 1L)).thenReturn(false);

        notifier.notifyClusterUpdate("cluster1", 1, "20201030");

        Mockito.verify(drNotifier, Mockito.timeout(1000))
                .notifyClusterUpdate("cluster1", null, 1L, "20201030");
        Mockito.verify(sentinelNotifier, Mockito.never()).notifyClusterUpdate(Mockito.anyString(), Mockito.any(),
                Mockito.anyLong(), Mockito.anyString());
    }

    @Test
    public void testNotifyClusterDeleted() {
        Mockito.when(drNotifier.needNotify("cluster1", null, 1L)).thenReturn(true);
        Mockito.when(sentinelNotifier.needNotify("cluster1", null, 1L)).thenReturn(false);

        notifier.notifyClusterDelete("cluster1", 1);

        Mockito.verify(drNotifier, Mockito.timeout(1000)).notifyClusterDelete("cluster1", null, 1L);
    }

    @Test
    public void testNotifyClusterChangeForSentinelBeacon() {
        Mockito.when(drNotifier.needNotify("cluster1", null, 1L)).thenReturn(false);
        Mockito.when(sentinelNotifier.needNotify("cluster1", null, 1L)).thenReturn(true);

        notifier.notifyClusterUpdate("cluster1", 1, "20201030");

        Mockito.verify(sentinelNotifier, Mockito.timeout(1000))
                .notifyClusterUpdate("cluster1", null, 1L, "20201030");
        Mockito.verify(drNotifier, Mockito.never()).notifyClusterUpdate(Mockito.anyString(), Mockito.any(),
                Mockito.anyLong(), Mockito.anyString());
    }

    @Test
    public void testNotifyClusterChangeWithDc() {
        Mockito.when(drNotifier.needNotify("cluster1", "jq", 1L)).thenReturn(true);
        Mockito.when(sentinelNotifier.needNotify("cluster1", "jq", 1L)).thenReturn(false);

        notifier.notifyClusterUpdate("cluster1", "jq", 1, "20201030");

        Mockito.verify(drNotifier, Mockito.timeout(1000))
                .notifyClusterUpdate("cluster1", "jq", 1L, "20201030");
    }

    @Test
    public void testSkipWhenBothNotifiersDecline() {
        Mockito.when(drNotifier.needNotify("cluster1", null, 1L)).thenReturn(false);
        Mockito.when(sentinelNotifier.needNotify("cluster1", null, 1L)).thenReturn(false);

        notifier.notifyClusterUpdate("cluster1", 1, "20201030");
        notifier.notifyClusterDelete("cluster1", 1);

        Mockito.verify(drNotifier, Mockito.never()).notifyClusterUpdate(Mockito.anyString(), Mockito.any(),
                Mockito.anyLong(), Mockito.anyString());
        Mockito.verify(sentinelNotifier, Mockito.never()).notifyClusterUpdate(Mockito.anyString(), Mockito.any(),
                Mockito.anyLong(), Mockito.anyString());
    }
}
