package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@RunWith(MockitoJUnitRunner.class)
public class KeeperContainerServiceTest {

    private KeeperConfig keeperConfig = new DefaultKeeperConfig();

	@Mock
    private LeaderElectorManager leaderElectorManager;
    @Mock
    private KeeperContainerConfig keeperContainerConfig;
    @Mock
    private ComponentRegistry componentRegistry;
    @Mock
    private KeepersMonitorManager keepersMonitorManager;
    
    private KeeperContainerService keeperContainerService;
    private String someCluster;
    private String someShard;
    private int somePort;
    private KeeperTransMeta someKeeperTransMeta;
    private KeeperMeta someKeeperMeta;

    @Before
    public void setUp() throws Exception {
        keeperContainerService = new KeeperContainerService();

        ReflectionTestUtils.setField(keeperContainerService, "leaderElectorManager", leaderElectorManager);
        ReflectionTestUtils.setField(keeperContainerService, "leaderElectorManager", leaderElectorManager);
        ReflectionTestUtils.setField(keeperContainerService, "keeperContainerConfig", keeperContainerConfig);
        ReflectionTestUtils.setField(keeperContainerService, "keeperConfig", keeperConfig);
        ReflectionTestUtils.setField(keeperContainerService, "keepersMonitorManager", keepersMonitorManager);

        someCluster = "someCluster";
        someShard = "someShard";
        somePort = 6789;

        someKeeperMeta = new KeeperMeta();
        someKeeperMeta.setPort(somePort);
        someKeeperTransMeta = new KeeperTransMeta();
        someKeeperTransMeta.setClusterId(someCluster);
        someKeeperTransMeta.setShardId(someShard);
        someKeeperTransMeta.setKeeperMeta(someKeeperMeta);

        when(keeperContainerConfig.getReplicationStoreDir()).thenReturn(System.getProperty("user.dir"));

        ReflectionTestUtils.setField(ComponentRegistryHolder.class, "componentRegistry", componentRegistry);
    }

    @Test
    public void testAddKeeper() throws Exception {
        RedisKeeperServer redisKeeperServer = keeperContainerService.add(someKeeperTransMeta);

        verify(componentRegistry, times(1)).add(redisKeeperServer);
        assertEquals(someCluster, redisKeeperServer.getClusterId());
        assertEquals(someShard, redisKeeperServer.getShardId());
        assertEquals(somePort, redisKeeperServer.getListeningPort());
    }

    @Test(expected = RedisKeeperRuntimeException.class)
    public void testAddKeeperWithSameClusterAndShardMultipleTimes() throws Exception {
        keeperContainerService.add(someKeeperTransMeta);
        keeperContainerService.add(someKeeperTransMeta);
    }

    @Test(expected = RedisKeeperRuntimeException.class)
    public void testAddKeeperWithSamePortMultipleTimes() throws Exception {
        String anotherShard = "anotherShard";

        KeeperMeta anotherKeeperMeta = new KeeperMeta();
        anotherKeeperMeta.setPort(somePort);
        KeeperTransMeta anotherKeeperTransMeta = new KeeperTransMeta();
        anotherKeeperTransMeta.setKeeperMeta(anotherKeeperMeta);
        anotherKeeperTransMeta.setClusterId(someCluster);
        anotherKeeperTransMeta.setShardId(anotherShard);

        keeperContainerService.add(someKeeperTransMeta);
        keeperContainerService.add(anotherKeeperTransMeta);
    }

    @Test
    public void testAddKeeperWithError() throws Exception {
        Exception someException = new Exception("some error happened");
        when(componentRegistry.add(any(RedisKeeperServer.class))).thenThrow(someException);

        Exception cause = null;
        try {
            keeperContainerService.add(someKeeperTransMeta);
        } catch (RedisKeeperRuntimeException ex) {
            cause = (Exception) ex.getCause();
        }

        assertEquals(someException, cause);
    }

}