package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.simpleserver.AbstractIoActionFactory;
import com.ctrip.xpipe.tuple.Pair;
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
public class TfsPrepareReleaseCommandTest extends AbstractMetaServerTest {

    @Mock
    private DcMetaCache dcMetaCache;

    private UnitTestServerConfig config;
    private TfsShardContext shardContext;

    @Before
    public void setUp() {
        config = new UnitTestServerConfig();
        shardContext = new TfsShardContext(1L, 1L);
        when(dcMetaCache.getKeeperContainer(any(KeeperMeta.class))).thenAnswer(invocation -> {
            KeeperMeta keeperMeta = invocation.getArgument(0);
            KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
            keeperContainerMeta.setId(keeperMeta.getKeeperContainerId());
            keeperContainerMeta.setTfsFsId("fs-42");
            return keeperContainerMeta;
        });
    }

    @Test
    public void testPrepareFailTriggersForceCloseDir() throws Exception {
        KeeperMeta oldTfs = keeper("127.0.0.1", 7101);
        KeeperMeta newActive = keeper("127.0.0.1", 7102);

        startServer(oldTfs.getPort(), new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                sleep(TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI * 2);
                return "+OK\r\n".getBytes();
            }
        });

        AtomicBoolean forceCloseCalled = new AtomicBoolean(false);
        TfsGateway gateway = (fsId, dirPath) -> forceCloseCalled.set(true);

        new TfsPrepareReleaseCommand(shardContext, oldTfs, newActive, getXpipeNettyClientKeyedObjectPool(), dcMetaCache, config,
                scheduled, executors, gateway).execute()
                .get(TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI * 3L, TimeUnit.MILLISECONDS);

        Assert.assertTrue(forceCloseCalled.get());
    }

    @Test
    public void testPrepareSuccessSkipsForceCloseDir() throws Exception {
        KeeperMeta oldTfs = keeper("127.0.0.1", 7103);
        KeeperMeta newActive = keeper("127.0.0.1", 7104);

        startServer(oldTfs.getPort(), new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                return "+OK\r\n".getBytes();
            }
        });

        AtomicBoolean forceCloseCalled = new AtomicBoolean(false);
        TfsGateway gateway = (fsId, dirPath) -> forceCloseCalled.set(true);

        new TfsPrepareReleaseCommand(shardContext, oldTfs, newActive, getXpipeNettyClientKeyedObjectPool(), dcMetaCache, config,
                scheduled, executors, gateway).execute()
                .get(2000, TimeUnit.MILLISECONDS);

        Assert.assertFalse(forceCloseCalled.get());
    }

    private KeeperMeta keeper(String ip, int port) {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setIp(ip);
        keeperMeta.setPort(port);
        keeperMeta.setKeeperContainerId(1L);
        return keeperMeta;
    }
}
