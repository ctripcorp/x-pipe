package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.entity.CheckerReportSituation;
import com.ctrip.xpipe.redis.console.keeper.entity.DcCheckerReportMsg;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.console.service.impl.RedisServiceImpl;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class KeeperContainerUsedInfoMsgCollectorTest {

    @Mock
    private MetaCache metaCache;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private CheckerConfig config;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private RedisServiceImpl redisService;

    @InjectMocks
    private KeeperContainerUsedInfoMsgCollector collector;

    @Mock
    private MetricProxy metricProxy;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetDcRedisMsg() {
        when(config.getKeeperCheckerIntervalMilli()).thenReturn(3000);
        Map<String, Map<Integer, Pair<Map<HostPort, RedisMsg>, Date>>> redisMsgCache = new HashMap<>();
        Map<Integer, Pair<Map<HostPort, RedisMsg>, Date>> dcCache = new HashMap<>();
        Map<HostPort, RedisMsg> msgMap = new HashMap<>();
        msgMap.put(new HostPort("127.0.0.1", 6379), new RedisMsg(100, 200, 300));
        dcCache.put(1, new Pair<>(msgMap, new Date()));
        redisMsgCache.put("dc1", dcCache);
        collector.redisMsgCache = redisMsgCache;

        DcCheckerReportMsg result = collector.getDcRedisMsg("dc1");

        assertEquals(1, result.getRedisMsg().size());
        assertEquals(1, result.getCheckerReportSituation().getReportedIndex().size());
        assertEquals(100, result.getRedisMsg().values().iterator().next().getInPutFlow());
    }

    @Test
    public void testRedisMsgMap2KeeperContainerUsedInfoModelsUtil() throws ResourceNotFoundException {
        // Setup
        Map<HostPort, RedisMsg> redisMsgMap = new HashMap<>();
        redisMsgMap.put(new HostPort("127.0.0.1", 6379), new RedisMsg(1024, 2048, 0));

        XpipeMeta xpipeMeta = Mockito.mock(XpipeMeta.class);

        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
        Map<String, DcMeta> dc1 = Mockito.mock(Map.class);
        when(xpipeMeta.getDcs()).thenReturn(dc1);
        DcMeta dc2 = Mockito.mock(DcMeta.class);

        // Test
        Map<String, KeeperContainerUsedInfoModel> result = collector.redisMsgMap2KeeperContainerUsedInfoModelsUtil(redisMsgMap);

        // Verify
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testReportKeeperData() throws MetricProxyException {
        // Setup
        KeeperContainerUsedInfoModel model = new KeeperContainerUsedInfoModel("127.0.0.1", "dc1", 1, 1);
        model.setOrg("org1");
        model.setDiskAvailable(true);
        model.setDiskSize(1024);
        model.setDiskUsed(512);
        model.setDetailInfo(new HashMap<>());
        model.getDetailInfo().put(new DcClusterShardKeeper("dc1", "cluster1", "shard1", false, 6379), new KeeperContainerUsedInfoModel.KeeperUsedInfo(1024, 2048, "127.0.0.1"));
        model.setActiveKeeperCount(1).setTotalKeeperCount(1).setActiveInputFlow(2048).setActiveRedisUsedMemory(1024).setTotalInputFlow(2048).setTotalRedisUsedMemory(1024);

        // Test
        collector.reportKeeperData(model, "keepercontainer.traffic", true);

        // Verify
        verify(metricProxy, times(1)).writeBinMultiDataPoint(any());
    }

    @Test
    public void testReportKeeperDataWithInactive() throws MetricProxyException {
        // Setup
        KeeperContainerUsedInfoModel model = new KeeperContainerUsedInfoModel("127.0.0.1", "dc1", 1, 1);
        model.setOrg("org1");
        model.setDiskAvailable(true);
        model.setDiskSize(1024);
        model.setDiskUsed(512);
        model.setDetailInfo(new HashMap<>());
        model.getDetailInfo().put(new DcClusterShardKeeper("dc1", "cluster1", "shard1", false, 6379), new KeeperContainerUsedInfoModel.KeeperUsedInfo(1024, 2048, "127.0.0.1"));
        model.setActiveKeeperCount(0).setTotalKeeperCount(1).setActiveInputFlow(0).setActiveRedisUsedMemory(0).setTotalInputFlow(2048).setTotalRedisUsedMemory(1024);

        // Test
        collector.reportKeeperData(model, "keepercontainer.traffic", false);

        // Verify
        verify(metricProxy, times(1)).writeBinMultiDataPoint(any());
    }

    @Test
    public void testDcCheckerReportMsgEquals() {
        Map<HostPort, RedisMsg> redisMsg1 = new HashMap<>();
        Map<HostPort, RedisMsg> redisMsg2 = new HashMap<>();
        List<Integer> reportedIndex1 = new ArrayList<>();
        reportedIndex1.add(1);
        reportedIndex1.add(2);
        reportedIndex1.add(3);
        List<Integer> reportedIndex2 = new ArrayList<>();
        reportedIndex2.add(4);
        reportedIndex2.add(5);
        reportedIndex2.add(6);

        CheckerReportSituation situation1 = new CheckerReportSituation("dc1", reportedIndex1, 5);
        CheckerReportSituation situation2 = new CheckerReportSituation("dc2", reportedIndex2, 8);

        DcCheckerReportMsg dcCheckerReportMsg1 = new DcCheckerReportMsg(redisMsg1, situation1);
        DcCheckerReportMsg dcCheckerReportMsg2 = new DcCheckerReportMsg(redisMsg2, situation1);
        DcCheckerReportMsg dcCheckerReportMsg3 = new DcCheckerReportMsg(redisMsg1, situation2);

        assertEquals(dcCheckerReportMsg1, dcCheckerReportMsg2); // Same redisMsg and situation
        assertNotEquals(dcCheckerReportMsg1, dcCheckerReportMsg3); // Different situation
    }

    @Test
    public void testDcCheckerReportMsgHashCode() {
        Map<HostPort, RedisMsg> redisMsg = new HashMap<>();
        List<Integer> reportedIndex = new ArrayList<>();
        reportedIndex.add(1);
        reportedIndex.add(2);
        reportedIndex.add(3);
        CheckerReportSituation situation = new CheckerReportSituation("dc1", reportedIndex, 5);

        DcCheckerReportMsg dcCheckerReportMsg = new DcCheckerReportMsg(redisMsg, situation);

        assertEquals(dcCheckerReportMsg.hashCode(), dcCheckerReportMsg.hashCode());
    }

    @Test
    public void testDcCheckerReportMsgToString() {
        Map<HostPort, RedisMsg> redisMsg = new HashMap<>();
        List<Integer> reportedIndex = new ArrayList<>();
        reportedIndex.add(1);
        reportedIndex.add(2);
        reportedIndex.add(3);
        CheckerReportSituation situation = new CheckerReportSituation("dc1", reportedIndex, 5);

        DcCheckerReportMsg dcCheckerReportMsg = new DcCheckerReportMsg(redisMsg, situation);

        String expectedToString = "{checkerReportSituation=" + situation + ", redisMsg=" + redisMsg + "}";
        assertEquals(expectedToString, dcCheckerReportMsg.toString());
    }

    @Test
    public void testCheckerReportSituationEquals() {
        List<Integer> reportedIndex1 = new ArrayList<>();
        reportedIndex1.add(1);
        reportedIndex1.add(2);
        reportedIndex1.add(3);
        List<Integer> reportedIndex2 = new ArrayList<>();
        reportedIndex2.add(1);
        reportedIndex2.add(2);
        reportedIndex2.add(3);

        CheckerReportSituation situation1 = new CheckerReportSituation("dc1", reportedIndex1, 5);
        CheckerReportSituation situation2 = new CheckerReportSituation("dc1", reportedIndex2, 5);
        CheckerReportSituation situation3 = new CheckerReportSituation("dc2", reportedIndex1, 5);

        assertEquals(situation1, situation2); // Same dc, reportedIndex and allIndexCount
        assertNotEquals(situation1, situation3); // Different dc
    }

    @Test
    public void testCheckerReportSituationHashCode() {
        List<Integer> reportedIndex = new ArrayList<>();
        reportedIndex.add(1);
        reportedIndex.add(2);
        reportedIndex.add(3);

        CheckerReportSituation situation = new CheckerReportSituation("dc1", reportedIndex, 5);

        assertEquals(situation.hashCode(), situation.hashCode());
    }

    @Test
    public void testCheckerReportSituationToString() {
        List<Integer> reportedIndex = new ArrayList<>();
        reportedIndex.add(1);
        reportedIndex.add(2);
        reportedIndex.add(3);

        CheckerReportSituation situation = new CheckerReportSituation("dc1", reportedIndex, 5);

        String expectedToString = "{dc='dc1', reportedIndex=[1, 2, 3], allIndexCount=5}";
        assertEquals(expectedToString, situation.toString());
    }

}