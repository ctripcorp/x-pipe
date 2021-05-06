package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.KeyedOneThreadMutexableTaskExecutor;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.job.ChangePrimaryDcJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Feb 26, 2020
 */
public class DefaultChangePrimaryDcActionTest extends AbstractTest {

    @Mock
    private KeyedOneThreadMutexableTaskExecutor<Pair<String, String>> clusterShardExecutors;

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private SentinelManager sentinelManager;

    @Mock
    private MultiDcService multiDcService;

    @Mock
    private OffsetWaiter offsetWaiter;

    @Mock
    private CurrentClusterServer currentClusterServer;

    @Mock
    private MetaServerConfig metaServerConfig;

    private DefaultChangePrimaryDcAction action;

    private MetaServerConsoleService.PrimaryDcChangeMessage expectedResult;

    @Before
    public void beforeDefaultChangePrimaryDcActionTest() {
        MockitoAnnotations.initMocks(this);
        expectedResult = new MetaServerConsoleService.PrimaryDcChangeMessage("Succeed", "127.0.0.1", 12345);
        action = new DefaultChangePrimaryDcAction() {
            @Override
            protected ChangePrimaryDcJob createChangePrimaryDcJob(ChangePrimaryDcAction changePrimaryDcAction, String clusterId,
                                                                  String shardId, String newPrimaryDc, MasterInfo masterInfo) {
                return new ChangePrimaryDcJob(changePrimaryDcAction, clusterId, shardId, newPrimaryDc, masterInfo) {
                    @Override
                    protected void doExecute() throws Exception {
                        future().setSuccess(expectedResult);
                    }
                };
            }
        };
        doNothing().when(clusterShardExecutors).clearAndExecute(any(), any());
        when(metaServerConfig.getWaitforOffsetMilli()).thenReturn(1000);
        action.setExecutors(executors).setScheduled(scheduled).setClusterShardExecutors(clusterShardExecutors)
        .setCurrentClusterServer(currentClusterServer).setOffsetWaiter(offsetWaiter).setMultiDcService(multiDcService)
        .setCurrentMetaManager(currentMetaManager).setDcMetaCache(dcMetaCache).setSentinelManager(sentinelManager)
                .setMetaServerConfig(metaServerConfig);
    }

    @Test
    public void testChangePrimaryDcFirstTimeout() {
        when(currentMetaManager.hasCluster(anyString())).thenReturn(true);
        when(dcMetaCache.getCurrentDc()).thenReturn("SHAOY");
        MetaServerConsoleService.PrimaryDcChangeMessage result = action.changePrimaryDc("cluster", "shard", "SHAOY", new MasterInfo());
        Assert.assertEquals(expectedResult, result);
    }

    @Test
    public void testChangePrimaryDcNotHavingCluster() {
        when(currentMetaManager.hasCluster(anyString())).thenReturn(false);
        when(dcMetaCache.getCurrentDc()).thenReturn("SHAOY");
        MetaServerConsoleService.PrimaryDcChangeMessage result = action.changePrimaryDc("cluster", "shard", "SHAOY", new MasterInfo());
        Assert.assertEquals(MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.FAIL, result.getErrorType());
        logger.info("\n{}", result.getErrorMessage());
    }

}