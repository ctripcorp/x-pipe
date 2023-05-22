package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.console.migration.auto.BeaconSystem;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author lishanglin
 * date 2021/1/19
 */
@RunWith(MockitoJUnitRunner.class)
public class UnknownClusterExcludeJobTest extends AbstractConsoleTest {

    @Mock
    private MonitorService monitorService;

    private Set<String> expectedClusters;

    @Before
    public void setupUnknownClusterExcludeJobTest() {
        expectedClusters = Sets.newHashSet("cluster1", "cluster2");
    }

    @Test
    public void testUnregisterUnknownCluster() throws Exception {
        UnknownClusterExcludeJob job = new UnknownClusterExcludeJob(expectedClusters, monitorService, 5);
        Mockito.when(monitorService.fetchAllClusters(Mockito.anyString())).thenReturn(Sets.newHashSet("cluster1", "cluster3", "cluster4"));
        Set<String> excludesClusters = job.execute().get();

        Assert.assertEquals(Sets.newHashSet("cluster3", "cluster4"), excludesClusters);
        Mockito.verify(monitorService, Mockito.times(2)).unregisterCluster(Mockito.anyString(), Mockito.anyString());
        Mockito.verify(monitorService).unregisterCluster(BeaconSystem.getDefault().getSystemName(), "cluster3");
        Mockito.verify(monitorService).unregisterCluster(BeaconSystem.getDefault().getSystemName(), "cluster4");
    }

    @Test(expected = TooManyNeedExcludeClusterException.class)
    public void testTooManyClusterToUnregister() throws Throwable {
        UnknownClusterExcludeJob job = new UnknownClusterExcludeJob(expectedClusters, monitorService, 1);
        Mockito.when(monitorService.fetchAllClusters(Mockito.anyString())).thenReturn(Sets.newHashSet("cluster1", "cluster3", "cluster4"));

        try {
            job.execute().get();
        } catch (ExecutionException executionException) {
            throw executionException.getCause();
        }
    }

}
