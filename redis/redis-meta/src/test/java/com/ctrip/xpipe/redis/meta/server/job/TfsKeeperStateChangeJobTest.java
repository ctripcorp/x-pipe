package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.ctrip.xpipe.redis.meta.server.keeper.elect.KeeperRoleAssigner;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.tfs.TfsCommandConstants;
import com.ctrip.xpipe.redis.meta.server.tfs.TfsGateway;
import com.ctrip.xpipe.simpleserver.AbstractIoActionFactory;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TfsKeeperStateChangeJobTest extends AbstractMetaServerTest {

    @Mock
    private DcMetaCache dcMetaCache;

    private UnitTestServerConfig config;
    private final List<String> callOrder = Collections.synchronizedList(new ArrayList<>());
    private static final long CLUSTER_DB_ID = 1L;
    private static final long SHARD_DB_ID = 2L;

    @Before
    public void setUp() {
        config = new UnitTestServerConfig();
        when(dcMetaCache.getKeeperContainer(any(KeeperMeta.class))).thenAnswer(invocation -> {
            KeeperMeta keeperMeta = invocation.getArgument(0);
            KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
            keeperContainerMeta.setId(keeperMeta.getKeeperContainerId());
            if (keeperMeta.getKeeperContainerId() >= 2L) {
                keeperContainerMeta.setDiskType("tfs");
                keeperContainerMeta.setTfsFsId("fs-42");
            } else {
                keeperContainerMeta.setDiskType("DEFAULT");
            }
            return keeperContainerMeta;
        });
    }

    @Test
    public void testTfsToBmSwitchSkipsPrepareDirectBackup() throws Exception {
        callOrder.clear();
        KeeperMeta oldTfs = keeper("127.0.0.1", 7001, 2L, true);
        KeeperMeta newBm = keeper("127.0.0.1", 7002, 1L, false);
        List<KeeperMeta> keepers = new LinkedList<>();
        keepers.add(oldTfs);
        keepers.add(newBm);

        Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(newBm, keepers, dcMetaCache);
        startKeeperServer(oldTfs.getPort());
        startKeeperServer(newBm.getPort());

        TfsKeeperStateChangeJob job = new TfsKeeperStateChangeJob(CLUSTER_DB_ID, SHARD_DB_ID, keepers, oldTfs,
                new Pair<>("localhost", randomPort()), null, getXpipeNettyClientKeyedObjectPool(),
                dcMetaCache, config, scheduled, executors, roles);
        job.execute().get(5000, TimeUnit.MILLISECONDS);

        Assert.assertEquals(2, callOrder.size());
        Assert.assertEquals(newBm.getPort() + ":ACTIVE", callOrder.get(0));
        Assert.assertEquals(oldTfs.getPort() + ":BACKUP", callOrder.get(1));
        Assert.assertFalse(callOrder.stream().anyMatch(entry -> entry.endsWith(":PREPARE")));
        Assert.assertEquals(KeeperState.BACKUP, roles.get(oldTfs));
        Assert.assertEquals(KeeperState.ACTIVE, roles.get(newBm));
    }

    @Test
    public void testTfsToTfsSwitchPrepareThenActive() throws Exception {
        callOrder.clear();
        KeeperMeta oldTfs = keeper("127.0.0.1", 7041, 2L, true);
        KeeperMeta newTfs = keeper("127.0.0.1", 7042, 2L, false);
        List<KeeperMeta> keepers = new LinkedList<>();
        keepers.add(oldTfs);
        keepers.add(newTfs);

        Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(newTfs, keepers, dcMetaCache);
        startKeeperServer(oldTfs.getPort());
        startKeeperServer(newTfs.getPort());

        TfsKeeperStateChangeJob job = new TfsKeeperStateChangeJob(CLUSTER_DB_ID, SHARD_DB_ID, keepers, oldTfs,
                new Pair<>("localhost", randomPort()), null, getXpipeNettyClientKeyedObjectPool(),
                dcMetaCache, config, scheduled, executors, roles);
        job.execute().get(5000, TimeUnit.MILLISECONDS);

        Assert.assertEquals(2, callOrder.size());
        Assert.assertEquals(oldTfs.getPort() + ":PREPARE", callOrder.get(0));
        Assert.assertEquals(newTfs.getPort() + ":ACTIVE", callOrder.get(1));
        Assert.assertEquals(KeeperState.PREPARE, roles.get(oldTfs));
        Assert.assertEquals(KeeperState.ACTIVE, roles.get(newTfs));
    }

    @Test
    public void testSkipPrepareWhenPreviousActiveNull() throws Exception {
        callOrder.clear();
        KeeperMeta oldTfs = keeper("127.0.0.1", 7011, 2L, false);
        KeeperMeta newBm = keeper("127.0.0.1", 7012, 1L, false);
        List<KeeperMeta> keepers = new LinkedList<>();
        keepers.add(oldTfs);
        keepers.add(newBm);

        Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(newBm, keepers, dcMetaCache);
        startKeeperServer(oldTfs.getPort());
        startKeeperServer(newBm.getPort());

        TfsKeeperStateChangeJob job = new TfsKeeperStateChangeJob(CLUSTER_DB_ID, SHARD_DB_ID, keepers, null,
                new Pair<>("localhost", randomPort()), null, getXpipeNettyClientKeyedObjectPool(),
                dcMetaCache, config, scheduled, executors, roles);
        job.execute().get(5000, TimeUnit.MILLISECONDS);

        Assert.assertEquals(2, callOrder.size());
        Assert.assertEquals(newBm.getPort() + ":ACTIVE", callOrder.get(0));
        Assert.assertEquals(oldTfs.getPort() + ":BACKUP", callOrder.get(1));
        Assert.assertFalse(callOrder.stream().anyMatch(entry -> entry.endsWith(":PREPARE")));
    }

    @Test
    public void testSkipPrepareWhenPreviousActiveNotTfs() throws Exception {
        callOrder.clear();
        KeeperMeta oldTfs = keeper("127.0.0.1", 7021, 2L, false);
        KeeperMeta newBm = keeper("127.0.0.1", 7022, 1L, false);
        KeeperMeta previousBmActive = keeper("127.0.0.1", 7023, 1L, true);
        List<KeeperMeta> keepers = new LinkedList<>();
        keepers.add(oldTfs);
        keepers.add(newBm);

        Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(newBm, keepers, dcMetaCache);
        startKeeperServer(oldTfs.getPort());
        startKeeperServer(newBm.getPort());

        TfsKeeperStateChangeJob job = new TfsKeeperStateChangeJob(CLUSTER_DB_ID, SHARD_DB_ID, keepers, previousBmActive,
                new Pair<>("localhost", randomPort()), null, getXpipeNettyClientKeyedObjectPool(),
                dcMetaCache, config, scheduled, executors, roles);
        job.execute().get(5000, TimeUnit.MILLISECONDS);

        Assert.assertEquals(2, callOrder.size());
        Assert.assertEquals(newBm.getPort() + ":ACTIVE", callOrder.get(0));
        Assert.assertEquals(oldTfs.getPort() + ":BACKUP", callOrder.get(1));
        Assert.assertFalse(callOrder.stream().anyMatch(entry -> entry.endsWith(":PREPARE")));
    }

    @Test
    public void testPrepareFailTriggersForceCloseDirThenActive() throws Exception {
        callOrder.clear();
        KeeperMeta oldTfs = keeper("127.0.0.1", 7031, 2L, true);
        KeeperMeta newTfs = keeper("127.0.0.1", 7032, 2L, false);
        List<KeeperMeta> keepers = new LinkedList<>();
        keepers.add(oldTfs);
        keepers.add(newTfs);

        Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(newTfs, keepers, dcMetaCache);
        startKeeperServer(oldTfs.getPort(), true);
        startKeeperServer(newTfs.getPort(), false);

        AtomicBoolean forceCloseCalled = new AtomicBoolean(false);
        TfsGateway gateway = (fsId, dirPath, podIp) -> {
            forceCloseCalled.set(true);
            callOrder.add("FORCE_CLOSE");
        };

        TfsKeeperStateChangeJob job = new TfsKeeperStateChangeJob(CLUSTER_DB_ID, SHARD_DB_ID, keepers, oldTfs,
                new Pair<>("localhost", randomPort()), null, getXpipeNettyClientKeyedObjectPool(),
                dcMetaCache, config, scheduled, executors, roles, gateway);
        job.execute().get(TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI * 5L, TimeUnit.MILLISECONDS);

        Assert.assertTrue(forceCloseCalled.get());
        Assert.assertEquals(3, callOrder.size());
        Assert.assertEquals(oldTfs.getPort() + ":PREPARE", callOrder.get(0));
        Assert.assertEquals("FORCE_CLOSE", callOrder.get(1));
        Assert.assertEquals(newTfs.getPort() + ":ACTIVE", callOrder.get(2));
    }

    @Test
    public void testBmActiveTfsBackupSwapReleasesOldSlotBeforeNewBackup() throws Exception {
        callOrder.clear();
        KeeperMeta bm = keeper("127.0.0.1", 7051, 1L, true).setPriority(1);
        KeeperMeta oldBackupTfs = keeper("127.0.0.1", 7052, 2L, false).setPriority(2);
        KeeperMeta newBackupTfs = keeper("127.0.0.1", 7053, 3L, false).setPriority(1);

        List<KeeperMeta> previousSurvive = new LinkedList<>();
        previousSurvive.add(bm);
        previousSurvive.add(oldBackupTfs);
        previousSurvive.add(newBackupTfs);

        KeeperMeta oldBackupAfter = keeper("127.0.0.1", 7052, 2L, false).setPriority(1);
        KeeperMeta newBackupAfter = keeper("127.0.0.1", 7053, 3L, false).setPriority(2);
        List<KeeperMeta> keepers = new LinkedList<>();
        keepers.add(bm);
        keepers.add(oldBackupAfter);
        keepers.add(newBackupAfter);

        Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(bm, keepers, dcMetaCache);
        Assert.assertEquals(KeeperState.PREPARE, roles.get(oldBackupAfter));
        Assert.assertEquals(KeeperState.BACKUP, roles.get(newBackupAfter));

        startKeeperServer(bm.getPort());
        startKeeperServer(oldBackupAfter.getPort());
        startKeeperServer(newBackupAfter.getPort());

        TfsKeeperStateChangeJob job = new TfsKeeperStateChangeJob(CLUSTER_DB_ID, SHARD_DB_ID, keepers, bm,
                previousSurvive, new Pair<>("localhost", randomPort()), null, getXpipeNettyClientKeyedObjectPool(),
                dcMetaCache, config, scheduled, executors, roles, null);
        job.execute().get(5000, TimeUnit.MILLISECONDS);

        Assert.assertEquals(oldBackupAfter.getPort() + ":PREPARE", callOrder.get(0));
        Assert.assertEquals(bm.getPort() + ":ACTIVE", callOrder.get(1));
        Assert.assertTrue(callOrder.contains(newBackupAfter.getPort() + ":BACKUP"));
        Assert.assertEquals(3, callOrder.size());
    }

    @Test
    public void testBmToTfsPromoteReleasesOldBackupBeforeNewActive() throws Exception {
        callOrder.clear();
        KeeperMeta bm = keeper("127.0.0.1", 7061, 1L, true).setPriority(1);
        KeeperMeta oldBackupTfs = keeper("127.0.0.1", 7062, 2L, false).setPriority(1);
        KeeperMeta promoteTfs = keeper("127.0.0.1", 7063, 3L, false).setPriority(0);

        List<KeeperMeta> previousSurvive = new LinkedList<>();
        previousSurvive.add(bm);
        previousSurvive.add(oldBackupTfs);
        previousSurvive.add(promoteTfs);

        KeeperMeta bmAfter = keeper("127.0.0.1", 7061, 1L, false).setPriority(0);
        KeeperMeta oldBackupAfter = keeper("127.0.0.1", 7062, 2L, false).setPriority(0);
        KeeperMeta newActiveTfs = keeper("127.0.0.1", 7063, 3L, true).setPriority(2);
        List<KeeperMeta> keepers = new LinkedList<>();
        keepers.add(bmAfter);
        keepers.add(oldBackupAfter);
        keepers.add(newActiveTfs);

        Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(newActiveTfs, keepers, dcMetaCache);
        Assert.assertEquals(KeeperState.PREPARE, roles.get(oldBackupAfter));
        Assert.assertEquals(KeeperState.BACKUP, roles.get(bmAfter));
        Assert.assertEquals(KeeperState.ACTIVE, roles.get(newActiveTfs));

        startKeeperServer(bmAfter.getPort());
        startKeeperServer(oldBackupAfter.getPort());
        startKeeperServer(newActiveTfs.getPort());

        TfsKeeperStateChangeJob job = new TfsKeeperStateChangeJob(CLUSTER_DB_ID, SHARD_DB_ID, keepers, bm,
                previousSurvive, new Pair<>("localhost", randomPort()), null, getXpipeNettyClientKeyedObjectPool(),
                dcMetaCache, config, scheduled, executors, roles, null);
        job.execute().get(5000, TimeUnit.MILLISECONDS);

        Assert.assertEquals(oldBackupAfter.getPort() + ":PREPARE", callOrder.get(0));
        Assert.assertEquals(newActiveTfs.getPort() + ":ACTIVE", callOrder.get(1));
        Assert.assertTrue(callOrder.contains(bmAfter.getPort() + ":BACKUP"));
        Assert.assertEquals(3, callOrder.size());
    }

    @Test
    public void testTfsActiveToBmWithPreviousSurviveSkipsPrepareWhenTargetBackup() throws Exception {
        callOrder.clear();
        KeeperMeta oldTfs = keeper("127.0.0.1", 7071, 2L, true);
        KeeperMeta newBm = keeper("127.0.0.1", 7072, 1L, false);
        List<KeeperMeta> previousSurvive = new LinkedList<>();
        previousSurvive.add(oldTfs);
        previousSurvive.add(newBm);

        List<KeeperMeta> keepers = new LinkedList<>();
        keepers.add(oldTfs);
        keepers.add(newBm);

        Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(newBm, keepers, dcMetaCache);
        startKeeperServer(oldTfs.getPort());
        startKeeperServer(newBm.getPort());

        TfsKeeperStateChangeJob job = new TfsKeeperStateChangeJob(CLUSTER_DB_ID, SHARD_DB_ID, keepers, oldTfs,
                previousSurvive, new Pair<>("localhost", randomPort()), null, getXpipeNettyClientKeyedObjectPool(),
                dcMetaCache, config, scheduled, executors, roles, null);
        job.execute().get(5000, TimeUnit.MILLISECONDS);

        Assert.assertEquals(2, callOrder.size());
        Assert.assertEquals(newBm.getPort() + ":ACTIVE", callOrder.get(0));
        Assert.assertEquals(oldTfs.getPort() + ":BACKUP", callOrder.get(1));
        Assert.assertFalse(callOrder.stream().anyMatch(entry -> entry.endsWith(":PREPARE")));
    }

    private void startKeeperServer(int port) throws Exception {
        startKeeperServer(port, false);
    }

    private void startKeeperServer(int port, boolean failPrepare) throws Exception {
        startServer(port, new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String state = parseKeeperSetState((String) readResult);
                if (state != null) {
                    callOrder.add(port + ":" + state);
                    if (failPrepare && "PREPARE".equals(state)) {
                        return "-ERR prepare failed\r\n".getBytes();
                    }
                }
                return "+OK\r\n".getBytes();
            }
        });
    }

    private String parseKeeperSetState(String request) {
        if (request == null) {
            return null;
        }
        if (request.contains("setstate PREPARE")) {
            return "PREPARE";
        }
        if (request.contains("setstate ACTIVE")) {
            return "ACTIVE";
        }
        if (request.contains("setstate BACKUP")) {
            return "BACKUP";
        }
        return null;
    }

    private KeeperMeta keeper(String ip, int port, long keeperContainerId, boolean active) {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setIp(ip);
        keeperMeta.setPort(port);
        keeperMeta.setKeeperContainerId(keeperContainerId);
        keeperMeta.setActive(active);
        return keeperMeta;
    }
}

