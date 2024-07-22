package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.MetaServerLocalDcSlotService;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.ShardCheckerHealthCheckModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.impl.MetaServerSlotServiceImpl;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardAllMetaModel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class MetaServerSlotServiceTest {

    @Mock
    public ConsoleServiceManager consoleManager;

    @Mock
    private ClusterService clusterService;

    @Mock
    private MetaServerLocalDcSlotService metaServerLocalDcSlotService;

    @InjectMocks
    private MetaServerSlotServiceImpl service;

    public static final String DC = "ptjq";

    public static final String CLUSTER = "cluster";

    public static final String SHARD = "shard";

    public static final String IP = "127.0.0.1";

    public static final int PORT = 6379;

    @Test
    public void testGetShardAllMeta() {
        ShardAllMetaModel shardAllCheckerGroupHealthCheck = Mockito.mock(ShardAllMetaModel.class);
        Mockito.when(consoleManager.getShardAllMeta(DC, CLUSTER, SHARD)).thenReturn(shardAllCheckerGroupHealthCheck);
        ShardAllMetaModel shardAllMeta = service.getShardAllMeta(DC, CLUSTER, SHARD);
        Assert.assertEquals(shardAllCheckerGroupHealthCheck, shardAllMeta);
    }

    @Test
    public void testGetLocalDcShardAllMeta() throws Exception{
        long clusterDbId = 123;
        ClusterTbl clusterTbl = Mockito.mock(ClusterTbl.class);
        Mockito.when(clusterTbl.getId()).thenReturn(clusterDbId);
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(clusterTbl);
        CommandFuture future = Mockito.mock(CommandFuture.class);
        Mockito.when(metaServerLocalDcSlotService.getLocalDcShardCurrentMeta(clusterDbId, CLUSTER, SHARD)).thenReturn(future);
        InterruptedException exception = Mockito.mock(InterruptedException.class);
        Mockito.when(future.get()).thenThrow(exception);
        HostPort metaServer = new HostPort("127.0.0.2", 8080);
        Mockito.when(metaServerLocalDcSlotService.getLocalDcManagerMetaServer(clusterDbId)).thenReturn(metaServer);
        ShardAllMetaModel shardAllMeta = service.getLocalDcShardAllMeta(DC, CLUSTER, SHARD);
        Assert.assertEquals(shardAllMeta.getMetaHost(), metaServer.getHost());
        Assert.assertEquals(shardAllMeta.getMetaPort(), metaServer.getPort());
        Assert.assertEquals(shardAllMeta.getErr(), exception);

    }

}
