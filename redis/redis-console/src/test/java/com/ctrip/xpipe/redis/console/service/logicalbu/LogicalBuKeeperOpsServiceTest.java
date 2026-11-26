package com.ctrip.xpipe.redis.console.service.logicalbu;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.LogicalBuModel;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.metaserver.DcMetaReloadResult;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class LogicalBuKeeperOpsServiceTest {

    private static final long BU = 101L;
    private static final long BU2 = 102L;
    private static final long BU_BM_ONLY = 103L;
    private static final String DC = "SHAJQ";
    private static final String DC2 = "SHARB";

    @InjectMocks
    private LogicalBuKeeperOpsService service;

    @Mock
    private LogicalBuService logicalBuService;
    @Mock
    private KeeperContainerService keeperContainerService;
    @Mock
    private DcService dcService;
    @Mock
    private MetaServerConsoleServiceManagerWrapper metaServerWrapper;
    @Mock
    private MetaServerConsoleService metaServer;
    @Mock
    private MetaServerConsoleService metaServer2;
    @Mock
    private KeeperContainerCheckerService keeperContainerChecker;
    @Mock
    private ConsoleConfig consoleConfig;

    @Before
    public void setUp() {
        service.init();
        Map<String, String> metaservers = new HashMap<>();
        metaservers.put(DC, "http://10.1.1.1:8080/");
        when(consoleConfig.getMetaservers()).thenReturn(metaservers);

        // BU 101: TFS KC 1, 2 in SHAJQ; BU 102: TFS KC 5 in SHAJQ, KC 6 in SHARB; BU 103: only BM
        when(keeperContainerService.findAll()).thenReturn(Arrays.asList(
                kc(1, 1, "TFS_1", BU), kc(2, 1, "TFS_1", BU), kc(3, 1, "DEFAULT", BU),
                kc(5, 1, "TFS_1", BU2), kc(6, 2, "TFS_1", BU2), kc(7, 1, "DEFAULT", BU_BM_ONLY)));
        when(dcService.findAllDcs()).thenReturn(Arrays.asList(
                new DcTbl().setId(1L).setDcName(DC), new DcTbl().setId(2L).setDcName(DC2)));
        when(logicalBuService.findById(BU)).thenReturn(bu(BU, 2));
        when(logicalBuService.findById(BU2)).thenReturn(bu(BU2, 2));
        when(logicalBuService.findById(BU_BM_ONLY)).thenReturn(bu(BU_BM_ONLY, null));
        when(logicalBuService.casPriority(anyLong(), any(), any())).thenReturn(true);
        when(metaServerWrapper.get(DC)).thenReturn(metaServer);
        when(metaServerWrapper.get(DC2)).thenReturn(metaServer2);
        when(metaServer.reloadDcMeta()).thenReturn(reload(DcMetaReloadResult.Status.APPLIED));
        when(metaServer2.reloadDcMeta()).thenReturn(reload(DcMetaReloadResult.Status.APPLIED));
        // KC 1 hosts replId 11, 12; KC 2 hosts replId 21
        when(keeperContainerChecker.getAllKeepers(DC, "10.0.0.1", null, 8080)).thenReturn(Arrays.asList(inst(11L), inst(12L)));
        when(keeperContainerChecker.getAllKeepers(DC, "10.0.0.2", null, 8080)).thenReturn(Collections.singletonList(inst(21L)));
        when(keeperContainerChecker.recoverHealthState(any(), any(), any(), anyInt(), any())).thenReturn("OK");
    }

    @After
    public void tearDown() {
        service.destroy();
    }

    @Test
    public void testSuccess() {
        LogicalBuOpResult r = run(BU);

        Assert.assertEquals(r.getFailures().toString(), LogicalBuOpResult.State.SUCCESS, r.getState());
        Assert.assertEquals(Integer.valueOf(2), r.getPriorityBefore());
        Assert.assertEquals(Integer.valueOf(0), r.getPriorityAfter());
        Assert.assertEquals(new TreeSet<>(Collections.singleton(DC)), r.getIdcs());
        verify(logicalBuService).casPriority(BU, 2, 0);
        verify(metaServer).reloadDcMeta();
        verify(metaServer2, never()).reloadDcMeta();
    }

    @Test
    public void testIdcsComeFromTfsContainers() {
        LogicalBuOpResult r = run(BU2);

        Assert.assertEquals(new TreeSet<>(Arrays.asList(DC, DC2)), r.getIdcs());
        verify(metaServer).reloadDcMeta();
        verify(metaServer2).reloadDcMeta();
    }

    @Test
    public void testBuWithoutTfsUpdatesDbButSkipsReload() {
        LogicalBuOpResult r = run(BU_BM_ONLY);

        Assert.assertEquals(LogicalBuOpResult.State.SUCCESS, r.getState());
        Assert.assertTrue(r.getIdcs().isEmpty());
        verify(logicalBuService).casPriority(BU_BM_ONLY, null, 0);
        verify(metaServer, never()).reloadDcMeta();
    }

    @Test
    public void testBuNotFound() {
        when(logicalBuService.findById(BU)).thenReturn(null);

        LogicalBuOpResult r = run(BU);

        assertOnlyFailure(r, OpLayer.CONSOLE, OpStage.LOAD, OpErrorCode.LOGICAL_BU_NOT_FOUND);
        Assert.assertEquals(LogicalBuOpResult.State.BLOCKED, r.getState());
        verify(logicalBuService, never()).casPriority(anyLong(), any(), any());
        verify(metaServer, never()).reloadDcMeta();
    }

    @Test
    public void testDbDownAtLoad() {
        when(keeperContainerService.findAll()).thenThrow(new ServerException("Load data failed.",
                new SQLNonTransientConnectionException("Communications link failure")));

        KeeperOpResult result = service.switchBM(new KeeperOpRequest().setLogicalBuIds(Arrays.asList(BU, BU2)));

        for (LogicalBuOpResult r : result.getResults()) {
            assertOnlyFailure(r, OpLayer.CONSOLE_DB, OpStage.LOAD, OpErrorCode.DB_UNAVAILABLE);
            Assert.assertEquals(LogicalBuOpResult.State.BLOCKED, r.getState());
        }
        verify(logicalBuService, never()).casPriority(anyLong(), any(), any());
    }

    @Test
    public void testPriorityStale() {
        when(logicalBuService.casPriority(BU, 2, 0)).thenReturn(false);
        when(logicalBuService.findById(BU)).thenReturn(bu(BU, 2), bu(BU, 1));

        LogicalBuOpResult r = run(BU);

        assertOnlyFailure(r, OpLayer.CONSOLE, OpStage.UPDATE_DB, OpErrorCode.PRIORITY_STALE);
        verify(metaServer, never()).reloadDcMeta();
    }

    @Test
    public void testDbTimeoutOnUpdateButCommitted() {
        when(logicalBuService.casPriority(BU, 2, 0)).thenThrow(new ServerException("x", new SQLTimeoutException("timeout")));
        when(logicalBuService.findById(BU)).thenReturn(bu(BU, 2), bu(BU, 0));

        Assert.assertEquals(LogicalBuOpResult.State.SUCCESS, run(BU).getState());
    }

    @Test
    public void testDbTimeoutOnUpdateAndRereadFails() {
        ServerException timeout = new ServerException("x", new SQLTimeoutException("timeout"));
        when(logicalBuService.casPriority(BU, 2, 0)).thenThrow(timeout);
        when(logicalBuService.findById(BU)).thenReturn(bu(BU, 2)).thenThrow(timeout);

        assertOnlyFailure(run(BU), OpLayer.CONSOLE_DB, OpStage.UPDATE_DB, OpErrorCode.DB_WRITE_UNKNOWN);
    }

    @Test
    public void testAlreadyZeroSkipsDbButStillReloads() {
        when(logicalBuService.findById(BU)).thenReturn(bu(BU, 0));

        Assert.assertEquals(LogicalBuOpResult.State.SUCCESS, run(BU).getState());
        verify(logicalBuService, never()).casPriority(anyLong(), any(), any());
        verify(metaServer).reloadDcMeta();
    }

    @Test
    public void testNullPriorityCasFromNull() {
        when(logicalBuService.findById(BU)).thenReturn(bu(BU, null));

        Assert.assertEquals(LogicalBuOpResult.State.SUCCESS, run(BU).getState());
        verify(logicalBuService).casPriority(BU, null, 0);
    }

    @Test
    public void testMetaServerUnreachable() {
        when(metaServer.reloadDcMeta()).thenThrow(new ResourceAccessException("I/O error", new ConnectException("Connection refused")));

        LogicalBuOpResult r = run(BU);

        assertOnlyFailure(r, OpLayer.META_SERVER, OpStage.NOTIFY_META, OpErrorCode.META_UNREACHABLE);
        // the configured address console called, not just the dc name
        Assert.assertEquals("10.1.1.1:8080", r.getFailures().get(0).getTarget());
        Assert.assertEquals(DC, r.getFailures().get(0).getScope());
        Assert.assertEquals("DB is updated anyway", Integer.valueOf(0), r.getPriorityAfter());
    }

    @Test
    public void testMetaServerReadTimeout() {
        when(metaServer.reloadDcMeta()).thenThrow(new ResourceAccessException("I/O error", new SocketTimeoutException("Read timed out")));

        assertOnlyFailure(run(BU), OpLayer.META_SERVER, OpStage.NOTIFY_META, OpErrorCode.META_TIMEOUT);
    }

    @Test
    public void testReloadUnsupportedByOldMetaServer() {
        when(metaServer.reloadDcMeta()).thenThrow(new HttpClientErrorException(HttpStatus.NOT_FOUND));

        assertOnlyFailure(run(BU), OpLayer.META_SERVER, OpStage.NOTIFY_META, OpErrorCode.META_RELOAD_UNSUPPORTED);
    }

    @Test
    public void testReloadPeerFailed() {
        when(metaServer.reloadDcMeta()).thenReturn(Arrays.asList(
                new DcMetaReloadResult(1, "10.0.0.1:8080", DcMetaReloadResult.Status.APPLIED, null),
                new DcMetaReloadResult(2, "10.0.0.2:8080", DcMetaReloadResult.Status.FAILED, "getDcMeta Overflow")));

        LogicalBuOpResult r = run(BU);

        assertOnlyFailure(r, OpLayer.META_SERVER, OpStage.NOTIFY_META, OpErrorCode.META_ERROR);
        Assert.assertTrue(r.getFailures().get(0).getDetail(), r.getFailures().get(0).getDetail().contains("server_2"));
        // only the instance that failed, by its own address
        Assert.assertEquals("10.0.0.2:8080", r.getFailures().get(0).getTarget());
    }

    @Test
    public void testReloadNotApplied() {
        when(metaServer.reloadDcMeta()).thenReturn(reload(DcMetaReloadResult.Status.SKIPPED_RECENT_MODIFY));

        assertOnlyFailure(run(BU), OpLayer.META_SERVER, OpStage.NOTIFY_META, OpErrorCode.META_RELOAD_NOT_APPLIED);
    }

    @Test
    public void testReloadNoChangeIsSuccess() {
        when(metaServer.reloadDcMeta()).thenReturn(reload(DcMetaReloadResult.Status.NO_CHANGE));

        Assert.assertEquals(LogicalBuOpResult.State.SUCCESS, run(BU).getState());
    }

    @Test
    public void testOneIdcFailureOnlyAffectsBusInThatIdc() {
        when(metaServer2.reloadDcMeta()).thenThrow(new ResourceAccessException("I/O error", new ConnectException("refused")));

        KeeperOpResult result = service.switchBM(new KeeperOpRequest().setLogicalBuIds(Arrays.asList(BU, BU2)));

        Assert.assertEquals(LogicalBuOpResult.State.SUCCESS, result.getResults().get(0).getState());
        LogicalBuOpResult bu2 = result.getResults().get(1);
        assertOnlyFailure(bu2, OpLayer.META_SERVER, OpStage.NOTIFY_META, OpErrorCode.META_UNREACHABLE);
        Assert.assertEquals(DC2, bu2.getFailures().get(0).getScope());
        Assert.assertEquals(1, result.getSuccessCount());
        Assert.assertEquals(1, result.getFailedCount());
    }

    @Test
    public void testOneReloadPerIdcForAllBus() {
        KeeperOpResult result = service.switchBM(new KeeperOpRequest().setLogicalBuIds(Arrays.asList(BU, BU2)));

        Assert.assertEquals(2, result.getSuccessCount());
        // both BUs are in SHAJQ: a single reload covers them
        verify(metaServer, times(1)).reloadDcMeta();
        verify(metaServer2, times(1)).reloadDcMeta();
        verify(keeperContainerService, times(1)).findAll();
    }

    @Test
    public void testBusUpdateInParallel() {
        CountDownLatch bothInUpdate = new CountDownLatch(2);
        doAnswer(inv -> {
            bothInUpdate.countDown();
            // only true if the other BU is updating at the same time
            return bothInUpdate.await(5, TimeUnit.SECONDS);
        }).when(logicalBuService).casPriority(anyLong(), any(), any());

        KeeperOpResult result = service.switchBM(new KeeperOpRequest().setLogicalBuIds(Arrays.asList(BU, BU2)));

        Assert.assertEquals(result.getResults().toString(), 2, result.getSuccessCount());
    }

    @Test
    public void testFailedBuDoesNotBlockReloadForOthers() {
        when(logicalBuService.casPriority(BU2, 2, 0)).thenReturn(false);
        when(logicalBuService.findById(BU2)).thenReturn(bu(BU2, 2), bu(BU2, 1));

        KeeperOpResult result = service.switchBM(new KeeperOpRequest().setLogicalBuIds(Arrays.asList(BU, BU2)));

        Assert.assertEquals(LogicalBuOpResult.State.SUCCESS, result.getResults().get(0).getState());
        Assert.assertEquals(LogicalBuOpResult.State.FAILED, result.getResults().get(1).getState());
        verify(metaServer, times(1)).reloadDcMeta();
        // SHARB is only touched by the failed BU
        verify(metaServer2, never()).reloadDcMeta();
    }

    @Test
    public void testSummaryGroupsByLayerAndCode() {
        when(metaServer.reloadDcMeta()).thenThrow(new ResourceAccessException("I/O error", new ConnectException("refused")));

        KeeperOpResult result = service.switchBM(new KeeperOpRequest().setLogicalBuIds(Arrays.asList(BU, BU)));

        Assert.assertEquals(1, result.getLogicalBuCount());
        Assert.assertEquals(1, result.getSummary().size());
        Assert.assertEquals(OpLayer.META_SERVER, result.getSummary().get(0).getLayer());
        Assert.assertEquals(OpErrorCode.META_UNREACHABLE, result.getSummary().get(0).getCode());
        Assert.assertEquals(1, result.getFailedCount());
    }

    @Test
    public void testRestoreClearsPriorityAndReloads() {
        when(logicalBuService.findById(BU)).thenReturn(bu(BU, 0));

        LogicalBuOpResult r = restore(BU);

        Assert.assertEquals(r.getFailures().toString(), LogicalBuOpResult.State.SUCCESS, r.getState());
        Assert.assertEquals(Integer.valueOf(0), r.getPriorityBefore());
        Assert.assertNull(r.getPriorityAfter());
        verify(logicalBuService).casPriority(BU, 0, null);
        verify(metaServer).reloadDcMeta();
    }

    @Test
    public void testRestoreAlreadyNullSkipsDbButStillReloads() {
        when(logicalBuService.findById(BU)).thenReturn(bu(BU, null));

        Assert.assertEquals(LogicalBuOpResult.State.SUCCESS, restore(BU).getState());
        verify(logicalBuService, never()).casPriority(anyLong(), any(), any());
        verify(metaServer).reloadDcMeta();
    }

    @Test
    public void testRestoreStale() {
        when(logicalBuService.findById(BU)).thenReturn(bu(BU, 0), bu(BU, 3));
        when(logicalBuService.casPriority(BU, 0, null)).thenReturn(false);

        LogicalBuOpResult r = restore(BU);

        assertOnlyFailure(r, OpLayer.CONSOLE, OpStage.UPDATE_DB, OpErrorCode.PRIORITY_STALE);
        verify(metaServer, never()).reloadDcMeta();
    }

    @Test
    public void testRestoreMetaServerFailureReported() {
        when(logicalBuService.findById(BU)).thenReturn(bu(BU, 0));
        when(metaServer.reloadDcMeta()).thenThrow(new ResourceAccessException("I/O error", new ConnectException("refused")));

        LogicalBuOpResult r = restore(BU);

        assertOnlyFailure(r, OpLayer.META_SERVER, OpStage.NOTIFY_META, OpErrorCode.META_UNREACHABLE);
        Assert.assertNull("DB is restored anyway", r.getPriorityAfter());
    }

    @Test
    public void testResetHealthRecoversEveryKeeperOfTfsContainers() {
        LogicalBuOpResult r = reset(BU);

        Assert.assertEquals(r.getFailures().toString(), LogicalBuOpResult.State.SUCCESS, r.getState());
        Assert.assertEquals(Integer.valueOf(3), r.getKeeperCount());
        Assert.assertEquals(Integer.valueOf(3), r.getSuccessKeeperCount());
        Assert.assertEquals(Integer.valueOf(2), r.getContainerCount());
        Assert.assertEquals(Integer.valueOf(2), r.getSuccessContainerCount());
        verify(keeperContainerChecker).recoverHealthState(DC, "10.0.0.1", null, 8080, 11L);
        verify(keeperContainerChecker).recoverHealthState(DC, "10.0.0.1", null, 8080, 12L);
        verify(keeperContainerChecker).recoverHealthState(DC, "10.0.0.2", null, 8080, 21L);
        // BM container KC 3 is never touched, priority and MetaServer untouched
        verify(keeperContainerChecker, never()).getAllKeepers(any(), eq("10.0.0.3"), any(), anyInt());
        verify(logicalBuService, never()).casPriority(anyLong(), any(), any());
        verify(metaServer, never()).reloadDcMeta();
    }

    @Test
    public void testResetHealthRejectedKeeperReported() {
        when(keeperContainerChecker.recoverHealthState(DC, "10.0.0.1", null, 8080, 12L))
                .thenReturn("Recover health for 12 rejected in state SICK");

        LogicalBuOpResult r = reset(BU);

        assertOnlyFailure(r, OpLayer.KEEPER, OpStage.RECOVER_HEALTH, OpErrorCode.RECOVERY_REJECTED);
        OpFailure f = r.getFailures().get(0);
        Assert.assertEquals("keepercontainer@10.0.0.1:8080", f.getTarget());
        Assert.assertEquals(DC + "/replId_12", f.getScope());
        Assert.assertTrue(f.getDetail(), f.getDetail().contains("SICK"));
        Assert.assertEquals(Integer.valueOf(2), r.getSuccessKeeperCount());
        // KC 1 has a rejected keeper, KC 2 is fine
        Assert.assertEquals(Integer.valueOf(2), r.getContainerCount());
        Assert.assertEquals(Integer.valueOf(1), r.getSuccessContainerCount());
    }

    @Test
    public void testResetHealthContainerUnreachable() {
        when(keeperContainerChecker.getAllKeepers(DC, "10.0.0.2", null, 8080))
                .thenThrow(new ResourceAccessException("I/O error", new ConnectException("Connection refused")));

        LogicalBuOpResult r = reset(BU);

        assertOnlyFailure(r, OpLayer.KEEPER, OpStage.RECOVER_HEALTH, OpErrorCode.KEEPER_UNREACHABLE);
        Assert.assertEquals("keepercontainer@10.0.0.2:8080", r.getFailures().get(0).getTarget());
        Assert.assertEquals(Integer.valueOf(2), r.getKeeperCount());
        Assert.assertEquals(Integer.valueOf(2), r.getContainerCount());
        Assert.assertEquals(Integer.valueOf(1), r.getSuccessContainerCount());
    }

    @Test
    public void testResetHealthRecoverTimeout() {
        when(keeperContainerChecker.recoverHealthState(DC, "10.0.0.2", null, 8080, 21L))
                .thenThrow(new ResourceAccessException("I/O error", new SocketTimeoutException("Read timed out")));

        assertOnlyFailure(reset(BU), OpLayer.KEEPER, OpStage.RECOVER_HEALTH, OpErrorCode.KEEPER_TIMEOUT);
    }

    @Test
    public void testResetHealthBuWithoutTfsIsNoop() {
        LogicalBuOpResult r = reset(BU_BM_ONLY);

        Assert.assertEquals(LogicalBuOpResult.State.SUCCESS, r.getState());
        Assert.assertEquals(Integer.valueOf(0), r.getKeeperCount());
        verify(keeperContainerChecker, never()).recoverHealthState(any(), any(), any(), anyInt(), any());
    }

    @Test
    public void testKeeperStateCountsContainersPerIdc() {
        // BU2: KC 5 in SHAJQ (one keeper DOWN) and KC 6 in SHARB (one RECOVERED_PENDING)
        when(keeperContainerChecker.getAllKeepers(DC, "10.0.0.5", null, 8080))
                .thenReturn(Arrays.asList(inst(51L, "HEALTHY"), inst(52L, "DOWN"), inst(53L, "RECOVERED_PENDING")));
        when(keeperContainerChecker.getAllKeepers(DC2, "10.0.0.6", null, 8080))
                .thenReturn(Arrays.asList(inst(61L, null), inst(62L, "RECOVERED_PENDING")));
        when(logicalBuService.findAll()).thenReturn(Arrays.asList(bu(BU, 2), bu(BU2, 2)));

        KeeperStateResult result = service.keeperState(new KeeperOpRequest().setLogicalBuIds(Arrays.asList(BU, BU2)));

        KeeperStateResult.BuState bu1 = result.getResults().get(0);
        Assert.assertEquals("bu-" + BU, bu1.getLogicalBuName());
        Assert.assertEquals(1, bu1.getIdcs().size());
        KeeperStateResult.IdcState jq = bu1.getIdcs().get(0);
        Assert.assertEquals(DC, jq.getIdc());
        // KC 1, 2 of BU 101: keepers without health state count as HEALTHY
        Assert.assertEquals(2, jq.getHealthy());

        KeeperStateResult.BuState bu2 = result.getResults().get(1);
        KeeperStateResult.IdcState bu2Jq = bu2.idc(DC);
        Assert.assertEquals(1, bu2Jq.getDown());
        Assert.assertEquals(0, bu2Jq.getPending());
        KeeperStateResult.ContainerInfo kc5 = bu2Jq.getContainers().get(0);
        Assert.assertEquals(3, kc5.getKeeperCount());
        Assert.assertEquals(1, kc5.getDownKeeperCount());
        Assert.assertEquals(1, kc5.getPendingKeeperCount());
        Assert.assertEquals(1, bu2.idc(DC2).getPending());
        verify(logicalBuService, never()).casPriority(anyLong(), any(), any());
    }

    @Test
    public void testSickKeeperCountsAsHealthy() {
        KeeperStateResult.ContainerInfo sickOnly = new KeeperStateResult.ContainerInfo();
        LogicalBuKeeperOpsService.classify(sickOnly, Arrays.asList(inst(1L, "SICK"), inst(2L, "HEALTHY")));
        Assert.assertEquals(KeeperStateResult.ContainerState.HEALTHY, sickOnly.getState());
        Assert.assertEquals(0, sickOnly.getPendingKeeperCount());

        KeeperStateResult.ContainerInfo sickAndPending = new KeeperStateResult.ContainerInfo();
        LogicalBuKeeperOpsService.classify(sickAndPending, Arrays.asList(inst(1L, "SICK"), inst(2L, "RECOVERED_PENDING")));
        Assert.assertEquals(KeeperStateResult.ContainerState.PENDING, sickAndPending.getState());
        Assert.assertEquals(1, sickAndPending.getPendingKeeperCount());
    }

    @Test
    public void testKeeperStateUnreachableContainerIsUnknownWithReason() {
        when(keeperContainerChecker.getAllKeepers(DC, "10.0.0.2", null, 8080))
                .thenThrow(new ResourceAccessException("I/O error", new ConnectException("Connection refused")));

        KeeperStateResult.IdcState jq = service.keeperState(new KeeperOpRequest()
                .setLogicalBuIds(Collections.singletonList(BU))).getResults().get(0).getIdcs().get(0);

        Assert.assertEquals(1, jq.getHealthy());
        Assert.assertEquals(1, jq.getUnknown());
        KeeperStateResult.ContainerInfo unknown = jq.getContainers().stream()
                .filter(c -> c.getState() == KeeperStateResult.ContainerState.UNKNOWN).findFirst().get();
        Assert.assertEquals("10.0.0.2:8080", unknown.getAddress());
        Assert.assertTrue(unknown.getReason(), unknown.getReason().startsWith("KEEPER_UNREACHABLE"));
    }

    @Test
    public void testKeeperStateDbDown() {
        when(keeperContainerService.findAll()).thenThrow(new ServerException("x",
                new SQLNonTransientConnectionException("Communications link failure")));

        KeeperStateResult.BuState bu = service.keeperState(new KeeperOpRequest()
                .setLogicalBuIds(Collections.singletonList(BU))).getResults().get(0);

        Assert.assertEquals(OpErrorCode.DB_UNAVAILABLE, bu.getFailure().getCode());
        Assert.assertTrue(bu.getIdcs().isEmpty());
    }

    private static KeeperInstanceMeta inst(Long replId, String health) {
        KeeperInstanceMeta meta = inst(replId);
        meta.setKeeperMeta(new KeeperMeta().setHealthState(health));
        return meta;
    }

    @Test
    public void testMetaServerTargetFallsBackToDcName() {
        Assert.assertEquals("10.1.1.1:8080", service.metaServerTarget(DC));
        // SHARB has no configured address
        Assert.assertEquals("metaserver@" + DC2, service.metaServerTarget(DC2));
    }

    @Test
    public void testDuplicateAndDeletedContainersFromFindAllAreIgnored() {
        // DAL find-all returns each container once per dc and keeps deleted ones
        when(keeperContainerService.findAll()).thenReturn(Arrays.asList(
                kc(1, 1, "TFS_1", BU), kc(1, 1, "TFS_1", BU), kc(2, 1, "TFS_1", BU), kc(2, 1, "TFS_1", BU),
                kc(9, 1, "TFS_1", BU).setDeleted(true)));

        KeeperStateResult.IdcState jq = service.keeperState(new KeeperOpRequest()
                .setLogicalBuIds(Collections.singletonList(BU))).getResults().get(0).getIdcs().get(0);
        Assert.assertEquals(2, jq.getContainers().size());

        LogicalBuOpResult r = reset(BU);
        Assert.assertEquals(Integer.valueOf(3), r.getKeeperCount());
        verify(keeperContainerChecker, times(1)).recoverHealthState(DC, "10.0.0.1", null, 8080, 11L);
        verify(keeperContainerChecker, never()).getAllKeepers(any(), eq("10.0.0.9"), any(), anyInt());
    }

    private LogicalBuOpResult reset(long logicalBuId) {
        return service.resetHealth(new KeeperOpRequest().setLogicalBuIds(Collections.singletonList(logicalBuId)))
                .getResults().get(0);
    }

    private static KeeperInstanceMeta inst(Long replId) {
        KeeperInstanceMeta meta = new KeeperInstanceMeta();
        meta.setReplId(replId);
        return meta;
    }

    private LogicalBuOpResult restore(long logicalBuId) {
        return service.switchRestore(new KeeperOpRequest().setLogicalBuIds(Collections.singletonList(logicalBuId)))
                .getResults().get(0);
    }

    private LogicalBuOpResult run(long logicalBuId) {
        return service.switchBM(new KeeperOpRequest().setLogicalBuIds(Collections.singletonList(logicalBuId)))
                .getResults().get(0);
    }

    private void assertOnlyFailure(LogicalBuOpResult r, OpLayer layer, OpStage stage, OpErrorCode code) {
        Assert.assertEquals(r.getFailures().toString(), 1, r.getFailures().size());
        OpFailure f = r.getFailures().get(0);
        Assert.assertEquals(f.toString(), layer, f.getLayer());
        Assert.assertEquals(f.toString(), stage, f.getStage());
        Assert.assertEquals(f.toString(), code, f.getCode());
        Assert.assertEquals(stage, r.getFailedStage());
    }

    private static List<DcMetaReloadResult> reload(DcMetaReloadResult.Status status) {
        return Collections.singletonList(new DcMetaReloadResult(1, "10.0.0.1:8080", status, null));
    }

    private static LogicalBuModel bu(long id, Integer priority) {
        return new LogicalBuModel().setId(id).setName("bu-" + id).setPriority(priority);
    }

    private static KeepercontainerTbl kc(long id, long dcId, String diskType, long buId) {
        return new KeepercontainerTbl().setKeepercontainerId(id).setKeepercontainerDc(dcId)
                .setKeepercontainerIp("10.0.0." + id).setKeepercontainerPort(8080)
                .setKeepercontainerDiskType(diskType).setLogicalBuId(buId);
    }
}
