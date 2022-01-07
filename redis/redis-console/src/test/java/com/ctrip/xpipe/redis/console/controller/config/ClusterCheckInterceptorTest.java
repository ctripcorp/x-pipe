package com.ctrip.xpipe.redis.console.controller.config;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Matchers.anyString;

/**
 * @author lishanglin
 * date 2021/6/24
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterCheckInterceptorTest extends AbstractConsoleTest {

    @Mock
    private MetaCache metaCache;

    @Mock
    private ClusterService clusterService;

    private ClusterCheckInterceptor interceptor;

    @Before
    public void setupClusterCheckInterceptorTest() {
        this.interceptor = new ClusterCheckInterceptor(metaCache, clusterService);
    }

    @Test
    public void testFindClusterTypeWithCache() {
        Mockito.when(metaCache.getClusterType("cluster1")).thenReturn(ClusterType.ONE_WAY);
        Assert.assertEquals(ClusterType.ONE_WAY, interceptor.tryParseClusterType("cluster1"));
        Mockito.verify(clusterService, Mockito.never()).find(anyString());
    }

    @Test
    public void testFindClusterTypeCacheMiss() {
        Mockito.when(metaCache.getClusterType(anyString())).thenThrow(new IllegalStateException("unfound cluster"));
        Mockito.when(clusterService.find("cluster1")).thenReturn(new ClusterTbl().setClusterType(ClusterType.ONE_WAY.name()));
        Assert.assertEquals(ClusterType.ONE_WAY, interceptor.tryParseClusterType("cluster1"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFindClusterTypeDBMiss() {
        Mockito.when(metaCache.getClusterType(anyString())).thenThrow(new IllegalStateException("unfound cluster"));
        Assert.assertEquals(ClusterType.ONE_WAY, interceptor.tryParseClusterType("cluster1"));
    }

}
