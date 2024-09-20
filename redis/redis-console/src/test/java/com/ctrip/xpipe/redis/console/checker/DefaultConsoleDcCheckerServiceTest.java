package com.ctrip.xpipe.redis.console.checker;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.model.CheckerRole;
import com.ctrip.xpipe.redis.console.checker.impl.DefaultConsoleDcCheckerService;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.ShardCheckerHealthCheckModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class DefaultConsoleDcCheckerServiceTest {

    @Mock
    public ConsoleServiceManager consoleManager;

    @Mock
    private ClusterService clusterService;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ConsoleCheckerGroupService consoleCheckerGroupService;

    @InjectMocks
    private DefaultConsoleDcCheckerService service;

    public static final String DC = "ptjq";

    public static final String CLUSTER = "cluster";

    public static final String SHARD = "shard";

    public static final String IP = "127.0.0.1";

    public static final int PORT = 6379;

    @Test
    public void testGetShardAllCheckerGroupHealthCheck() {
        List<ShardCheckerHealthCheckModel> list = new ArrayList<>();
        Mockito.when(consoleManager.getShardAllCheckerGroupHealthCheck("", DC, CLUSTER, SHARD)).thenReturn(list);
        List<ShardCheckerHealthCheckModel> shardAllCheckerGroupHealthCheck = service.getShardAllCheckerGroupHealthCheck(DC, CLUSTER, SHARD);
        Assert.assertEquals(list, shardAllCheckerGroupHealthCheck);
    }

    @Test
    public void testGetLocalDcShardAllCheckerGroupHealthCheck() throws Exception {
        long clusterDbId = 123;
        ClusterTbl clusterTbl = Mockito.mock(ClusterTbl.class);
        Mockito.when(clusterTbl.getId()).thenReturn(clusterDbId);
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(clusterTbl);
        List<RedisMeta> redisMetas = new ArrayList<>();
        RedisMeta redisMeta = Mockito.mock(RedisMeta.class);
        redisMetas.add(redisMeta);
        Mockito.when(redisMeta.getIp()).thenReturn(IP);
        Mockito.when(redisMeta.getPort()).thenReturn(PORT);
        Mockito.when(metaCache.getRedisOfDcClusterShard(DC, CLUSTER, SHARD)).thenReturn(redisMetas);
        CommandFuture future1 = Mockito.mock(CommandFuture.class);
        CommandFuture future2 = Mockito.mock(CommandFuture.class);
        Mockito.when(consoleCheckerGroupService.getAllHealthCheckInstance(clusterDbId, IP, PORT, false)).thenReturn(future1);
        Mockito.when(consoleCheckerGroupService.getAllHealthStates(clusterDbId, IP, PORT, false)).thenReturn(future2);
        HostPort checker = new HostPort("127.0.0.2", 8080);
        Map<HostPort, String> checkerActionMap = new HashMap<>();
        Map<HostPort, HEALTH_STATE> checkerHealthStateMap = new HashMap<>();;
        checkerActionMap.put(checker, "action");
        checkerHealthStateMap.put(checker, HEALTH_STATE.HEALTHY);
        Mockito.when(future1.get()).thenReturn(checkerActionMap);
        Mockito.when(future2.get()).thenReturn(checkerHealthStateMap);
        Mockito.when(consoleCheckerGroupService.getCheckerLeader(clusterDbId)).thenReturn(checker);
        List<ShardCheckerHealthCheckModel> localDcShardAllCheckerGroupHealthCheck = service.getLocalDcShardAllCheckerGroupHealthCheck(DC, CLUSTER, SHARD);
        Assert.assertEquals(localDcShardAllCheckerGroupHealthCheck.size(), 1);
        Assert.assertEquals(localDcShardAllCheckerGroupHealthCheck.get(0).getHost(), "127.0.0.2");
        Assert.assertEquals(localDcShardAllCheckerGroupHealthCheck.get(0).getPort(), 8080);
        Assert.assertEquals(localDcShardAllCheckerGroupHealthCheck.get(0).getCheckerRole(), CheckerRole.LEADER);
        Assert.assertEquals(localDcShardAllCheckerGroupHealthCheck.get(0).getInstances().get(0).getHost(), IP);
        Assert.assertEquals(localDcShardAllCheckerGroupHealthCheck.get(0).getInstances().get(0).getPort(), PORT);
        Assert.assertEquals(localDcShardAllCheckerGroupHealthCheck.get(0).getInstances().get(0).getActions(), "action");
        Assert.assertEquals(localDcShardAllCheckerGroupHealthCheck.get(0).getInstances().get(0).getState(), HEALTH_STATE.HEALTHY);
    }

}
