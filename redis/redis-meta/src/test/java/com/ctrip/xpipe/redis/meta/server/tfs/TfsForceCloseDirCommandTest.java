package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TfsForceCloseDirCommandTest extends AbstractMetaServerTest {

    @Mock
    private DcMetaCache dcMetaCache;

    private UnitTestServerConfig config;
    private TfsShardContext shardContext;

    @Before
    public void setUp() {
        config = new UnitTestServerConfig();
        shardContext = new TfsShardContext(1L, 1L);
    }

    @Test
    public void testForceCloseDirSuccess() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        TfsGateway gateway = (fsId, dirPath) -> {
            called.set(true);
            Assert.assertEquals("fs-42", fsId);
            Assert.assertEquals("/opt/data/100004376/rsd/replication_store_6380", dirPath);
        };
        KeeperMeta keeperMeta = keeper(6380, 1L);
        mockKeeperContainer(1L, "fs-42");

        new TfsForceCloseDirCommand(shardContext, keeperMeta, dcMetaCache, config, scheduled, executors, gateway).execute()
                .get(2000, TimeUnit.MILLISECONDS);

        Assert.assertTrue(called.get());
    }

    @Test
    public void testEmptyFsIdStillSuccess() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        TfsGateway gateway = (fsId, dirPath) -> called.set(true);
        KeeperMeta keeperMeta = keeper(6380, 1L);
        mockKeeperContainer(1L, null);

        new TfsForceCloseDirCommand(shardContext, keeperMeta, dcMetaCache, config, scheduled, executors, gateway).execute()
                .get(2000, TimeUnit.MILLISECONDS);

        Assert.assertFalse(called.get());
    }

    @Test
    public void testGatewayFailureStillSuccess() throws Exception {
        TfsGateway gateway = (fsId, dirPath) -> {
            throw new RuntimeException("gateway down");
        };
        KeeperMeta keeperMeta = keeper(6380, 1L);
        mockKeeperContainer(1L, "fs-42");

        var future = new TfsForceCloseDirCommand(shardContext, keeperMeta, dcMetaCache, config, scheduled, executors, gateway).execute();
        future.get(2000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(future.isSuccess());
    }

    @Test
    public void testGatewayTimeoutStillSuccess() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        TfsGateway gateway = (fsId, dirPath) -> {
            called.set(true);
            Thread.sleep(TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI * 3L);
        };
        KeeperMeta keeperMeta = keeper(6380, 1L);
        mockKeeperContainer(1L, "fs-42");

        long start = System.currentTimeMillis();
        new TfsForceCloseDirCommand(shardContext, keeperMeta, dcMetaCache, config, scheduled, executors, gateway).execute()
                .get(TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI * 2L, TimeUnit.MILLISECONDS);

        Assert.assertTrue(called.get());
        long elapsed = System.currentTimeMillis() - start;
        Assert.assertTrue("expected command to finish within step timeout, elapsed=" + elapsed,
                elapsed < TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI + 500);
    }

    @Test
    public void testEmptyDirPathStillSuccess() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        TfsGateway gateway = (fsId, dirPath) -> called.set(true);
        KeeperMeta keeperMeta = keeper(6380, 1L);
        mockKeeperContainer(1L, "fs-42");
        config = new UnitTestServerConfig() {
            @Override
            public String getTfsDirPathTemplate() {
                return null;
            }
        };

        new TfsForceCloseDirCommand(shardContext, keeperMeta, dcMetaCache, config, scheduled, executors, gateway).execute()
                .get(2000, TimeUnit.MILLISECONDS);

        Assert.assertFalse(called.get());
    }

    private KeeperMeta keeper(int port, long keeperContainerId) {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setPort(port);
        keeperMeta.setIp("127.0.0.1");
        keeperMeta.setKeeperContainerId(keeperContainerId);
        return keeperMeta;
    }

    private void mockKeeperContainer(long id, String tfsFsId) {
        when(dcMetaCache.getKeeperContainer(any(KeeperMeta.class))).thenAnswer(invocation -> {
            KeeperMeta keeperMeta = invocation.getArgument(0);
            KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
            keeperContainerMeta.setId(keeperMeta.getKeeperContainerId());
            keeperContainerMeta.setTfsFsId(tfsFsId);
            return keeperContainerMeta;
        });
    }
}
