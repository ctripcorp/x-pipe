package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author lishanglin
 * date 2021/1/19
 */
@RunWith(MockitoJUnitRunner.class)
public class UnknownClusterExcludeJobTest extends AbstractConsoleTest {

    @Mock
    private MonitorService monitorService0;
    @Mock
    private MonitorService monitorService1;
    private List<MonitorService> monitorServices;

    private Set<String> expectedClusters;

    @Before
    public void setupUnknownClusterExcludeJobTest() {
        expectedClusters = Sets.newHashSet("cluster1", "cluster2", "cluster6");
        monitorServices = Lists.newArrayList(monitorService0, monitorService1);
    }

    @Test
    public void testUnregisterUnknownCluster() throws Exception {
        UnknownClusterExcludeJob job = new UnknownClusterExcludeJob(expectedClusters, monitorServices, 5);
        Mockito.when(monitorService0.fetchAllClusters(Mockito.anyString())).thenReturn(Sets.newHashSet("cluster1", "cluster3", "cluster4"));
        Mockito.when(monitorService1.fetchAllClusters(Mockito.anyString())).thenReturn(Sets.newHashSet("cluster2", "cluster5"));
        Set<String> excludesClusters = job.execute().get();

        Assert.assertEquals(Sets.newHashSet("cluster3", "cluster4", "cluster5"), excludesClusters);
        Mockito.verify(monitorService0, Mockito.times(2)).unregisterCluster(Mockito.anyString(), Mockito.anyString());
        Mockito.verify(monitorService0).unregisterCluster(BeaconSystem.getDefault().getSystemName(), "cluster3");
        Mockito.verify(monitorService0).unregisterCluster(BeaconSystem.getDefault().getSystemName(), "cluster4");
        Mockito.verify(monitorService1, Mockito.times(1)).unregisterCluster(Mockito.anyString(), Mockito.anyString());
        Mockito.verify(monitorService1).unregisterCluster(BeaconSystem.getDefault().getSystemName(), "cluster5");
    }

    @Test(expected = TooManyNeedExcludeClusterException.class)
    public void testTooManyClusterToUnregister() throws Throwable {
        UnknownClusterExcludeJob job = new UnknownClusterExcludeJob(expectedClusters, monitorServices, 2);
        Mockito.when(monitorService0.fetchAllClusters(Mockito.anyString())).thenReturn(Sets.newHashSet("cluster1", "cluster4", "cluster7"));
        Mockito.when(monitorService1.fetchAllClusters(Mockito.anyString())).thenReturn(Sets.newHashSet("cluster5"));

        try {
            job.execute().get();
        } catch (ExecutionException executionException) {
            throw executionException.getCause();
        }
    }

}
