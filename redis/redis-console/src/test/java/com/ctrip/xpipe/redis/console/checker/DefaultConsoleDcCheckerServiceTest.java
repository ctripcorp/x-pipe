package com.ctrip.xpipe.redis.console.checker;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.model.CheckerRole;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.checker.impl.DefaultConsoleDcCheckerService;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.ShardCheckerHealthCheckModel;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class DefaultConsoleDcCheckerServiceTest {

    @Mock
    public ConsoleServiceManager consoleManager;

    @Mock
    private ClusterService clusterService;

    @Mock
    private DcService dcService;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ConsoleCheckerGroupService consoleCheckerGroupService;

    @Mock
    private AzGroupClusterRepository azGroupClusterRepository;

    @Mock
    private AzGroupCache azGroupCache;

    @InjectMocks
    private DefaultConsoleDcCheckerService service;

    public static final String DC = "ptjq";

    public static final String ACTIVE_DC = "fra";

    public static final String CLUSTER = "cluster";

    public static final String SHARD = "shard";

    public static final String IP = "127.0.0.1";

    public static final int PORT = 6379;

    private static final long CLUSTER_DB_ID = 123L;

    private static final long ACTIVE_DC_ID = 10L;

    private ClusterTbl oneWayCluster;

    private ClusterTbl heteroCluster;

    @Before
    public void setUp() {
        oneWayCluster = new ClusterTbl()
                .setId(CLUSTER_DB_ID)
                .setClusterType(ClusterType.ONE_WAY.toString())
                .setActivedcId(ACTIVE_DC_ID);

        heteroCluster = new ClusterTbl()
                .setId(CLUSTER_DB_ID)
                .setClusterType(ClusterType.HETERO.toString())
                .setActivedcId(0L);

        Mockito.when(dcService.getDcName(ACTIVE_DC_ID)).thenReturn(ACTIVE_DC);
    }

    // ---- getShardAllCheckerGroupHealthCheck (ONE_WAY) -------------------------

    @Test
    public void testGetShardAllCheckerGroupHealthCheck_clusterNotFound() {
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(null);
        Assert.assertNull(service.getShardAllCheckerGroupHealthCheck(DC, CLUSTER, SHARD));
    }

    @Test
    public void testGetShardAllCheckerGroupHealthCheck_oneWay_forwardToActiveDc() {
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(oneWayCluster);
        Mockito.when(metaCache.isCrossRegion(DC, ACTIVE_DC)).thenReturn(false);

        List<ShardCheckerHealthCheckModel> remoteResult = new ArrayList<>();
        Mockito.when(consoleManager.getShardAllCheckerGroupHealthCheck(ACTIVE_DC, DC, CLUSTER, SHARD))
                .thenReturn(remoteResult);

        List<ShardCheckerHealthCheckModel> result = service.getShardAllCheckerGroupHealthCheck(DC, CLUSTER, SHARD);
        Assert.assertEquals(remoteResult, result);
        Mockito.verify(consoleManager).getShardAllCheckerGroupHealthCheck(ACTIVE_DC, DC, CLUSTER, SHARD);
    }

    @Test
    public void testGetShardAllCheckerGroupHealthCheck_oneWay_crossRegion_forwardsBoth() {
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(oneWayCluster);
        Mockito.when(metaCache.isCrossRegion(DC, ACTIVE_DC)).thenReturn(true);

        List<ShardCheckerHealthCheckModel> localRegionResult = Collections.singletonList(
                new ShardCheckerHealthCheckModel("10.0.0.1", 8080, DC));
        List<ShardCheckerHealthCheckModel> activeRegionResult = Collections.singletonList(
                new ShardCheckerHealthCheckModel("10.0.0.2", 8080, ACTIVE_DC));
        Mockito.when(consoleManager.getShardAllCheckerGroupHealthCheck(DC, DC, CLUSTER, SHARD))
                .thenReturn(localRegionResult);
        Mockito.when(consoleManager.getShardAllCheckerGroupHealthCheck(ACTIVE_DC, DC, CLUSTER, SHARD))
                .thenReturn(activeRegionResult);

        List<ShardCheckerHealthCheckModel> result = service.getShardAllCheckerGroupHealthCheck(DC, CLUSTER, SHARD);
        Assert.assertEquals(2, result.size());
    }

    // ---- getShardAllCheckerGroupHealthCheck (HETERO) --------------------------

    @Test
    public void testGetShardAllCheckerGroupHealthCheck_hetero_oneWayAzGroup_containsDc_usesAzGroupActiveDc() {
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(heteroCluster);

        AzGroupClusterEntity azGroupCluster = buildAzGroupCluster(1L, ClusterType.ONE_WAY.toString(), ACTIVE_DC_ID);
        Mockito.when(azGroupClusterRepository.selectByClusterId(CLUSTER_DB_ID))
                .thenReturn(Collections.singletonList(azGroupCluster));

        AzGroupModel azGroup = new AzGroupModel(1L, "az-group", "region", new HashSet<>(Arrays.asList(DC, ACTIVE_DC)));
        Mockito.when(azGroupCache.getAzGroupById(1L)).thenReturn(azGroup);

        Mockito.when(metaCache.isCrossRegion(DC, ACTIVE_DC)).thenReturn(false);
        List<ShardCheckerHealthCheckModel> remoteResult = new ArrayList<>();
        Mockito.when(consoleManager.getShardAllCheckerGroupHealthCheck(ACTIVE_DC, DC, CLUSTER, SHARD))
                .thenReturn(remoteResult);

        List<ShardCheckerHealthCheckModel> result = service.getShardAllCheckerGroupHealthCheck(DC, CLUSTER, SHARD);
        Assert.assertEquals(remoteResult, result);
        Mockito.verify(consoleManager).getShardAllCheckerGroupHealthCheck(ACTIVE_DC, DC, CLUSTER, SHARD);
        Mockito.verify(dcService, Mockito.never()).getDcName(0L);
    }

    @Test
    public void testGetShardAllCheckerGroupHealthCheck_hetero_singleDcAzGroup_skipped_fallsBackToClusterActiveDc() {
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(
                new ClusterTbl().setId(CLUSTER_DB_ID).setClusterType(ClusterType.HETERO.toString()).setActivedcId(ACTIVE_DC_ID));

        AzGroupClusterEntity singleDcAzGroup = buildAzGroupCluster(2L, ClusterType.SINGLE_DC.toString(), 99L);
        Mockito.when(azGroupClusterRepository.selectByClusterId(CLUSTER_DB_ID))
                .thenReturn(Collections.singletonList(singleDcAzGroup));

        Mockito.when(metaCache.isCrossRegion(DC, ACTIVE_DC)).thenReturn(false);
        List<ShardCheckerHealthCheckModel> remoteResult = new ArrayList<>();
        Mockito.when(consoleManager.getShardAllCheckerGroupHealthCheck(ACTIVE_DC, DC, CLUSTER, SHARD))
                .thenReturn(remoteResult);

        List<ShardCheckerHealthCheckModel> result = service.getShardAllCheckerGroupHealthCheck(DC, CLUSTER, SHARD);
        Assert.assertEquals(remoteResult, result);
        // SINGLE_DC azGroupCluster should be skipped; falls back to clusterTbl.activedcId -> ACTIVE_DC_ID
        Mockito.verify(dcService).getDcName(ACTIVE_DC_ID);
    }

    @Test
    public void testGetShardAllCheckerGroupHealthCheck_hetero_oneWayAzGroup_dcNotInGroup_fallsBackToClusterActiveDc() {
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(
                new ClusterTbl().setId(CLUSTER_DB_ID).setClusterType(ClusterType.HETERO.toString()).setActivedcId(ACTIVE_DC_ID));

        AzGroupClusterEntity azGroupCluster = buildAzGroupCluster(1L, ClusterType.ONE_WAY.toString(), 99L);
        Mockito.when(azGroupClusterRepository.selectByClusterId(CLUSTER_DB_ID))
                .thenReturn(Collections.singletonList(azGroupCluster));

        // AZ group does NOT contain the queried DC
        AzGroupModel azGroup = new AzGroupModel(1L, "az-group", "region", new HashSet<>(Arrays.asList("other-dc1", "other-dc2")));
        Mockito.when(azGroupCache.getAzGroupById(1L)).thenReturn(azGroup);

        Mockito.when(metaCache.isCrossRegion(DC, ACTIVE_DC)).thenReturn(false);
        List<ShardCheckerHealthCheckModel> remoteResult = new ArrayList<>();
        Mockito.when(consoleManager.getShardAllCheckerGroupHealthCheck(ACTIVE_DC, DC, CLUSTER, SHARD))
                .thenReturn(remoteResult);

        service.getShardAllCheckerGroupHealthCheck(DC, CLUSTER, SHARD);
        Mockito.verify(dcService).getDcName(ACTIVE_DC_ID);
    }

    // ---- getLocalDcShardAllCheckerGroupHealthCheck ----------------------------

    @Test
    public void testGetLocalDcShardAllCheckerGroupHealthCheck_clusterNotFound() {
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(null);
        Assert.assertNull(service.getLocalDcShardAllCheckerGroupHealthCheck(DC, CLUSTER, SHARD));
    }

    @Test
    public void testGetLocalDcShardAllCheckerGroupHealthCheck_oneWay() throws Exception {
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(oneWayCluster);

        List<RedisMeta> redisMetas = new ArrayList<>();
        RedisMeta redisMeta = Mockito.mock(RedisMeta.class);
        redisMetas.add(redisMeta);
        Mockito.when(redisMeta.getIp()).thenReturn(IP);
        Mockito.when(redisMeta.getPort()).thenReturn(PORT);
        Mockito.when(metaCache.getRedisOfDcClusterShard(DC, CLUSTER, SHARD)).thenReturn(redisMetas);
        Mockito.when(metaCache.isCrossRegion(Mockito.anyString(), Mockito.eq(ACTIVE_DC))).thenReturn(false);

        CommandFuture future1 = Mockito.mock(CommandFuture.class);
        CommandFuture future2 = Mockito.mock(CommandFuture.class);
        Mockito.when(consoleCheckerGroupService.getAllHealthCheckInstance(CLUSTER_DB_ID, IP, PORT, false)).thenReturn(future1);
        Mockito.when(consoleCheckerGroupService.getAllHealthStates(CLUSTER_DB_ID, IP, PORT, false)).thenReturn(future2);

        HostPort checker = new HostPort("127.0.0.2", 8080);
        Map<HostPort, String> checkerActionMap = new HashMap<>();
        Map<HostPort, HEALTH_STATE> checkerHealthStateMap = new HashMap<>();
        checkerActionMap.put(checker, "action");
        checkerHealthStateMap.put(checker, HEALTH_STATE.HEALTHY);
        Mockito.when(future1.get()).thenReturn(checkerActionMap);
        Mockito.when(future2.get()).thenReturn(checkerHealthStateMap);
        Mockito.when(consoleCheckerGroupService.getCheckerLeader(CLUSTER_DB_ID)).thenReturn(checker);

        List<ShardCheckerHealthCheckModel> result = service.getLocalDcShardAllCheckerGroupHealthCheck(DC, CLUSTER, SHARD);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("127.0.0.2", result.get(0).getHost());
        Assert.assertEquals(8080, result.get(0).getPort());
        Assert.assertEquals(CheckerRole.LEADER, result.get(0).getCheckerRole());
        Assert.assertEquals(IP, result.get(0).getInstances().get(0).getHost());
        Assert.assertEquals(PORT, result.get(0).getInstances().get(0).getPort());
        Assert.assertEquals("action", result.get(0).getInstances().get(0).getActions());
        Assert.assertEquals(HEALTH_STATE.HEALTHY, result.get(0).getInstances().get(0).getState());
    }

    @Test
    public void testGetLocalDcShardAllCheckerGroupHealthCheck_hetero_usesAzGroupActiveDcForCrossRegion() throws Exception {
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(heteroCluster);

        AzGroupClusterEntity azGroupCluster = buildAzGroupCluster(1L, ClusterType.ONE_WAY.toString(), ACTIVE_DC_ID);
        Mockito.when(azGroupClusterRepository.selectByClusterId(CLUSTER_DB_ID))
                .thenReturn(Collections.singletonList(azGroupCluster));
        AzGroupModel azGroup = new AzGroupModel(1L, "az-group", "region", new HashSet<>(Arrays.asList(DC, ACTIVE_DC)));
        Mockito.when(azGroupCache.getAzGroupById(1L)).thenReturn(azGroup);

        List<RedisMeta> redisMetas = Collections.singletonList(buildRedisMeta(IP, PORT));
        Mockito.when(metaCache.getRedisOfDcClusterShard(DC, CLUSTER, SHARD)).thenReturn(redisMetas);
        Mockito.when(metaCache.isCrossRegion(Mockito.anyString(), Mockito.eq(ACTIVE_DC))).thenReturn(true);

        CommandFuture future1 = Mockito.mock(CommandFuture.class);
        CommandFuture future2 = Mockito.mock(CommandFuture.class);
        Mockito.when(consoleCheckerGroupService.getAllHealthCheckInstance(CLUSTER_DB_ID, IP, PORT, true)).thenReturn(future1);
        Mockito.when(consoleCheckerGroupService.getAllHealthStates(CLUSTER_DB_ID, IP, PORT, true)).thenReturn(future2);
        Mockito.when(future1.get()).thenReturn(new HashMap<>());
        Mockito.when(future2.get()).thenReturn(new HashMap<>());
        Mockito.when(consoleCheckerGroupService.getCheckerLeader(CLUSTER_DB_ID)).thenReturn(null);

        service.getLocalDcShardAllCheckerGroupHealthCheck(DC, CLUSTER, SHARD);

        // isCrossRegion must be called with the AZ-group-resolved activeDc, not clusterTbl.activedcId (0)
        Mockito.verify(metaCache).isCrossRegion(Mockito.anyString(), Mockito.eq(ACTIVE_DC));
        Mockito.verify(consoleCheckerGroupService).getAllHealthCheckInstance(CLUSTER_DB_ID, IP, PORT, true);
    }

    // ---- helpers --------------------------------------------------------------

    private AzGroupClusterEntity buildAzGroupCluster(Long azGroupId, String type, Long activeAzId) {
        AzGroupClusterEntity entity = new AzGroupClusterEntity();
        entity.setAzGroupId(azGroupId);
        entity.setClusterId(CLUSTER_DB_ID);
        entity.setAzGroupClusterType(type);
        entity.setActiveAzId(activeAzId);
        return entity;
    }

    private RedisMeta buildRedisMeta(String ip, int port) {
        RedisMeta meta = Mockito.mock(RedisMeta.class);
        Mockito.when(meta.getIp()).thenReturn(ip);
        Mockito.when(meta.getPort()).thenReturn(port);
        return meta;
    }
}
