package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Dec 27, 2017
 */
public class SentinelUpdateControllerTest {

    private Logger logger = LoggerFactory.getLogger(SentinelUpdateControllerTest.class);

    @Mock
    private ClusterService clusterService;

    @Mock
    private AzGroupClusterRepository azGroupClusterRepository;

    @Mock
    private DcClusterShardService dcClusterShardService;

    @Mock
    private SentinelService sentinelService;

    @Mock
    private SentinelBalanceService sentinelBalanceService;

    @Mock
    private DcService dcService;

    @InjectMocks
    SentinelUpdateController controller = new SentinelUpdateController();

    private String[] clusters = {"cluster1", "cluster2", "cluster3", "cluster4"};

    private JsonCodec jsonCodec = new JsonCodec(true, true);

    private final String clusterName = "cluster";
    private final String dcName = "dc";
    private final String shardName = "shard";

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testConvert2SentinelTbl() throws Exception {
        when(dcService.find(anyString())).thenReturn(new DcTbl().setId(1));
        SentinelModel sentinelModel = new SentinelModel().setDcName("JQ")
                .setDesc("test").setSentinels(Arrays.asList(new HostPort("127.0.0.1", 6379),
                        new HostPort("127.0.0.1", 6380), new HostPort("127.0.0.1", 6381)));
        SetinelTbl setinelTbl = controller.convert2SentinelTbl(sentinelModel);
        Assert.assertEquals(1, setinelTbl.getDcId());
        Assert.assertEquals("test", setinelTbl.getSetinelDescription());
        Assert.assertEquals("127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381", setinelTbl.getSetinelAddress());
    }

    @Test
    public void testJsonShow() {
        SentinelUsageModel usageModel1 = new SentinelUsageModel("SHAJQ", 2)
                .addSentinelUsage("127.0.0.1:6379,127.0.0.1:6380", 100,"")
                .addSentinelUsage("192.168.0.1:6379,192.168.0.1:6380", 200,"");
        SentinelUsageModel usageModel2 = new SentinelUsageModel("SHAOY", 2)
                .addSentinelUsage("127.0.0.2:6381,127.0.0.1:6382", 150,"")
                .addSentinelUsage("192.168.0.2:6381,192.168.0.1:6382", 150,"");
        Map<String, SentinelUsageModel> map = Maps.newHashMapWithExpectedSize(2);
        map.put("SHAJQ", usageModel1);
        map.put("SHAOY", usageModel2);
        JsonCodec jsonTool = new JsonCodec(true, true);
        System.out.println(jsonTool.encode(map));
    }

    @Test
    public void testSentinelUpdate() {
        RetMessage message = controller.updateSentinelAddr(new SentinelGroupModel());
        logger.info("{}", message.getState());
    }


    private void mockSentinel() {
        ClusterTbl cluster = new ClusterTbl().setId(1L).setClusterName(clusterName).setClusterType(ClusterType.ONE_WAY.toString()).setTag("");
        DcClusterShardTbl dcClusterShard = new DcClusterShardTbl().setSetinelId(1L);
        when(clusterService.find(clusterName)).thenReturn(cluster);
        when(dcClusterShardService.find(dcName, clusterName, shardName)).thenReturn(dcClusterShard);

        SentinelInstanceModel instanceModel1 = new SentinelInstanceModel().setSentinelIp("127.0.0.1").setSentinelPort(7000);
        SentinelInstanceModel instanceModel2 = new SentinelInstanceModel().setSentinelIp("127.0.0.1").setSentinelPort(7001);
        SentinelGroupModel model1 = new SentinelGroupModel().setSentinelGroupId(1L).setSentinels(Collections.singletonList(instanceModel1));
        SentinelGroupModel model2 = new SentinelGroupModel().setSentinelGroupId(2L).setSentinels(Collections.singletonList(instanceModel2));
        when(sentinelBalanceService.selectSentinel(dcName, ClusterType.ONE_WAY, "")).thenReturn(model1);
        when(sentinelBalanceService.selectSentinel(dcName, ClusterType.SINGLE_DC, "")).thenReturn(model2);
    }

    @Test
    public void testUpdateDcShardSentinels1() {
        mockSentinel();
        AzGroupClusterEntity azGroupCluster = new AzGroupClusterEntity().setAzGroupClusterType(ClusterType.ONE_WAY.toString());
        when(azGroupClusterRepository.selectByClusterIdAndAz(1L, dcName)).thenReturn(azGroupCluster);

        RetMessage ret = controller.updateDcShardSentinels(dcName, clusterName, shardName);
        Assert.assertEquals(ret.getState(), RetMessage.SUCCESS_STATE);
        Assert.assertEquals(ret.getMessage(), "current sentinel is suitable, no change");
    }

    @Test
    public void testUpdateDcShardSentinel2() {
        mockSentinel();
        AzGroupClusterEntity azGroupCluster = new AzGroupClusterEntity().setAzGroupClusterType(ClusterType.SINGLE_DC.toString());
        when(azGroupClusterRepository.selectByClusterIdAndAz(1L, dcName)).thenReturn(azGroupCluster);

        RetMessage ret = controller.updateDcShardSentinels(dcName, clusterName, shardName);
        Assert.assertEquals(ret.getState(), RetMessage.SUCCESS_STATE);
        Assert.assertEquals(ret.getMessage(), "sentinel changed to 127.0.0.1:7001");
    }
}