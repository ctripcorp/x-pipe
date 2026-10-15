package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.config.ReplDelayConfigCache;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class KeeperContainerServiceLocalIpTest extends AbstractTest {

    @Mock private KeeperConfig keeperConfig;
    @Mock private LeaderElectorManager leaderElectorManager;
    @Mock private KeeperContainerConfig keeperContainerConfig;
    @Mock private KeepersMonitorManager keepersMonitorManager;
    @Mock private KeeperResourceManager resourceManager;
    @Mock private ContainerResourceManager containerResourceManager;
    @Mock private GeneralRedisOpParser redisOpParser;
    @Mock private SyncRateManager syncRateManager;
    @Mock private ReplDelayConfigCache replDelayConfigCache;
    @Mock private ComponentRegistry componentRegistry;

    private KeeperContainerService service;

    @Before
    public void setUp() throws Exception {
        service = spy(new KeeperContainerService());
        ReflectionTestUtils.setField(service, "keeperConfig", keeperConfig);
        ReflectionTestUtils.setField(service, "leaderElectorManager", leaderElectorManager);
        ReflectionTestUtils.setField(service, "keeperContainerConfig", keeperContainerConfig);
        ReflectionTestUtils.setField(service, "keepersMonitorManager", keepersMonitorManager);
        ReflectionTestUtils.setField(service, "resourceManager", resourceManager);
        ReflectionTestUtils.setField(service, "containerResourceManager", containerResourceManager);
        ReflectionTestUtils.setField(service, "redisOpParser", redisOpParser);
        ReflectionTestUtils.setField(service, "syncRateManager", syncRateManager);
        ReflectionTestUtils.setField(service, "replDelayConfigCache", replDelayConfigCache);
        ReflectionTestUtils.setField(ComponentRegistryHolder.class, "componentRegistry", componentRegistry);
        when(keeperContainerConfig.getReplicationStoreDir()).thenReturn(System.getProperty("user.dir"));
        when(keeperContainerConfig.isTfsMode()).thenReturn(false);
        when(containerResourceManager.applyPort(anyInt())).thenReturn(true);
    }

    @Test
    public void testContainerKeeperUsesLocalIp() {
        KeeperTransMeta transMeta = keeperTransMeta("0.0.0.0", "keeper-pod-a-0");
        doReturn("10.2.3.15").when(service).getLocalIp();

        service.add(transMeta);

        Assert.assertEquals("10.2.3.15", transMeta.getKeeperMeta().getIp());
    }

    @Test
    public void testPhysicalKeeperKeepsConfiguredIp() {
        KeeperTransMeta transMeta = keeperTransMeta("192.168.1.10", null);

        service.add(transMeta);

        Assert.assertEquals("192.168.1.10", transMeta.getKeeperMeta().getIp());
        verify(service, never()).getLocalIp();
    }

    @Test
    public void testContainerKeeperRejectsBlankLocalIpBeforeApplyingPort() {
        assertInvalidLocalIp("");
    }

    @Test
    public void testContainerKeeperRejectsPlaceholderLocalIpBeforeApplyingPort() {
        assertInvalidLocalIp("0.0.0.0");
    }

    @Test
    public void testContainerKeeperRejectsNullLocalIpBeforeApplyingPort() {
        assertInvalidLocalIp(null);
    }

    @Test
    public void testContainerKeeperRejectsWhitespaceLocalIpBeforeApplyingPort() {
        assertInvalidLocalIp("   ");
    }

    private void assertInvalidLocalIp(String localIp) {
        KeeperTransMeta transMeta = keeperTransMeta("0.0.0.0", "keeper-pod-a-0");
        doReturn(localIp).when(service).getLocalIp();

        try {
            service.add(transMeta);
            Assert.fail("invalid local ip should be rejected");
        } catch (RedisKeeperRuntimeException expected) {
            verify(containerResourceManager, never()).applyPort(anyInt());
        }
    }

    private KeeperTransMeta keeperTransMeta(String ip, String hostname) {
        KeeperMeta keeperMeta = new KeeperMeta().setIp(ip).setHostname(hostname).setPort(6789);
        KeeperTransMeta transMeta = new KeeperTransMeta();
        transMeta.setReplId(1L);
        transMeta.setClusterDbId(1L);
        transMeta.setShardDbId(1L);
        transMeta.setKeeperMeta(keeperMeta);
        return transMeta;
    }
}
