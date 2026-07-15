package com.ctrip.xpipe.redis.console.service.vo;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.exception.DcMetaBuilderException;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DcMetaBuilderAzGroupExceptionTest {

    @Mock private AzGroupCache azGroupCache;
    @Mock private RedisMetaService redisMetaService;
    @Mock private DcClusterService dcClusterService;
    @Mock private ClusterMetaService clusterMetaService;
    @Mock private DcClusterShardService dcClusterShardService;
    @Mock private DcService dcService;
    @Mock private AzGroupClusterRepository azGroupClusterRepository;
    @Mock private ConsoleConfig consoleConfig;

    private DcMetaBuilder builder;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        builder = new DcMetaBuilder(new HashMap<>(), Collections.emptyList(),
                Collections.singleton(ClusterType.ONE_WAY.toString()),
                Executors.newSingleThreadExecutor(),
                redisMetaService, dcClusterService, clusterMetaService,
                dcClusterShardService, dcService, azGroupClusterRepository, azGroupCache,
                new DefaultRetryCommandFactory(), consoleConfig);

        Map<Long, String> dcNameMap = new HashMap<>();
        dcNameMap.put(1L, "sha");
        dcNameMap.put(2L, "sgp");
        builder.setDcNameMap(dcNameMap);

        DcClusterTbl dcClusterTbl = mock(DcClusterTbl.class);
        when(dcClusterTbl.getClusterId()).thenReturn(100L);
        when(dcClusterTbl.getDcId()).thenReturn(1L);
        builder.setCluster2DcClusterMap(Collections.singletonMap(100L,
                Collections.singletonList(dcClusterTbl)));

        builder.cluster2AzGroupClusterMap = Collections.singletonMap(100L,
                Collections.singletonList(new AzGroupClusterEntity()
                        .setClusterId(100L)
                        .setAzGroupId(9L)
                        .setAzGroupClusterType(ClusterType.ONE_WAY.toString())));
    }

    // getAzGroupCluster: azGroup 缓存未命中时应抛 DcMetaBuilderException
    @Test
    public void testGetAzGroupCluster_azGroupNotInCache_throwsDcMetaBuilderException() {
        when(azGroupCache.getAzGroupById(9L)).thenReturn(null);

        DcMetaBuilder.BuildDcMetaCommand cmd = builder.createBuildDcMetaCommand();
        try {
            cmd.getAzGroupCluster(100L, 1L);
            Assert.fail("expected DcMetaBuilderException");
        } catch (DcMetaBuilderException e) {
            Assert.assertTrue(e.getMessage().contains("azGroupId=9"));
        }
    }

    // getAzGroupCluster: azGroup 正常命中且包含目标 dc，返回对应 entity
    @Test
    public void testGetAzGroupCluster_azGroupFound_returnsEntity() {
        AzGroupModel azGroup = new AzGroupModel(9L, "SGP", "SGP", Arrays.asList("sha", "sgp"));
        when(azGroupCache.getAzGroupById(9L)).thenReturn(azGroup);

        DcMetaBuilder.BuildDcMetaCommand cmd = builder.createBuildDcMetaCommand();
        AzGroupClusterEntity result = cmd.getAzGroupCluster(100L, 1L);

        Assert.assertNotNull(result);
        Assert.assertEquals(9L, (long) result.getAzGroupId());
    }

    // getOrCreateClusterMeta: azGroup 缓存未命中时应抛 DcMetaBuilderException
    @Test
    public void testGetOrCreateClusterMeta_azGroupNotInCache_throwsDcMetaBuilderException() {
        when(azGroupCache.getAzGroupById(9L)).thenReturn(null);

        ClusterTbl cluster = mock(ClusterTbl.class);
        when(cluster.getId()).thenReturn(100L);
        when(cluster.getClusterName()).thenReturn("test-cluster");
        when(cluster.getClusterType()).thenReturn(ClusterType.HETERO.toString());
        when(cluster.getClusterOrgId()).thenReturn(0L);
        when(cluster.getClusterAdminEmails()).thenReturn("");

        AzGroupClusterEntity azGroupCluster = new AzGroupClusterEntity()
                .setAzGroupId(9L)
                .setAzGroupClusterType(ClusterType.ONE_WAY.toString());

        try {
            builder.getOrCreateClusterMeta(new DcMeta().setId("sha"), 1L, cluster, null, azGroupCluster);
            Assert.fail("expected DcMetaBuilderException");
        } catch (DcMetaBuilderException e) {
            Assert.assertTrue(e.getMessage().contains("azGroupId=9"));
        }
    }
}
