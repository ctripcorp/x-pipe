package com.ctrip.xpipe.redis.console.controller.api.data;

import com.alibaba.fastjson.JSON;

import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.SentinelModel;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Dec 27, 2017
 */
public class SentinelUpdateControllerTest {

    @Mock
    private ClusterService clusterService;

    @Mock
    private SentinelService sentinelService;

    @Mock
    private DcService dcService;

    @InjectMocks
    SentinelUpdateController controller = new SentinelUpdateController();

    private String[] clusters = {"cluster1", "cluster2", "cluster3", "cluster4"};

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        when(clusterService.reBalanceSentinels(anyInt())).thenReturn(Arrays.asList(clusters));
        when(clusterService.reBalanceSentinels(0)).thenReturn(Collections.emptyList());
    }

    @Test
    public void validateMock() {
        Assert.assertEquals(Collections.emptyList(), clusterService.reBalanceSentinels(0));
        Assert.assertEquals(Arrays.asList(clusters), clusterService.reBalanceSentinels(2));
    }

    @Test
    public void testReBalanceSentinels() throws Exception {
        RetMessage message = RetMessage.createSuccessMessage("clusters: " + JSON.toJSONString(Arrays.asList(clusters)));
        Assert.assertEquals(message.getMessage(), controller.reBalanceSentinels(3).getMessage());
    }

    @Test
    public void reBalanceSentinels1() throws Exception {
        RetMessage message = RetMessage.createSuccessMessage("clusters: " + JSON.toJSONString(Collections.emptyList()));
        Assert.assertEquals(message.getMessage(), controller.reBalanceSentinels().getMessage());
    }

    @Test
    public void reBalanceSentinels2() throws Exception {
        String expectedMessage = "Expected Message";
        when(clusterService.reBalanceSentinels(-1)).thenThrow(new RuntimeException(expectedMessage));
        RetMessage message = controller.reBalanceSentinels(-1);
        Assert.assertEquals(-1, message.getState());
        Assert.assertEquals(expectedMessage, message.getMessage());
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
}