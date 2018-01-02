package com.ctrip.xpipe.redis.console.controller.api.data;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Dec 27, 2017
 */
public class SentinelUpdateControllerTest {

    @Mock
    private ClusterService clusterService;

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
}