package com.ctrip.xpipe.redis.checker.controller;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

/**
 * @author lishanglin
 * date 2021/3/16
 */
@RunWith(MockitoJUnitRunner.class)
public class CheckerHealthControllerTest extends AbstractCheckerTest {

    @InjectMocks
    private CheckerHealthController healthController;

    @Mock
    private HealthCheckInstanceManager instanceManager;

    @Mock
    private RedisHealthCheckInstance instance;

    @Mock
    private AbstractHealthCheckAction action;

    @Mock
    private HealthCheckActionListener listener;

    @Mock
    private HealthCheckActionController controller;

    @Mock
    private ClusterHealthCheckInstance clusterInstance;

    private String instanceName = "test-instance";
    private String clusterName = "test-cluster";
    private String actionName = "test-action";
    private String listenerName = "test-listener";
    private String controllerName = "test-controller";
    private String expectedResult = String.format("{\"info\":\"%s\",\"actions\":[{\"name\":\"%s\",\"listeners\":[\"%s\"],\"controllers\":[\"%s\"],\"redisCheckRules\":[]}]}",
            instanceName, actionName, listenerName, controllerName);
    private String expectedClusterResult = String.format("{\"info\":\"%s\",\"actions\":[{\"name\":\"%s\",\"listeners\":[\"%s\"],\"controllers\":[\"%s\"],\"redisCheckRules\":[]}]}",
            clusterName, actionName, listenerName, controllerName);

    @Before
    public void setupHealthControllerTest() {
        Mockito.when(instanceManager.findRedisHealthCheckInstance(Mockito.any())).thenReturn(instance);
        Mockito.when(instance.getHealthCheckActions()).thenReturn(Collections.singletonList(action));
        Mockito.when(action.getListeners()).thenReturn(Collections.singletonList(listener));
        Mockito.when(action.getControllers()).thenReturn(Collections.singletonList(controller));
        Mockito.when(instance.toString()).thenReturn(instanceName);
        Mockito.when(action.toString()).thenReturn(actionName);
        Mockito.when(listener.toString()).thenReturn(listenerName);
        Mockito.when(controller.toString()).thenReturn(controllerName);

        Mockito.when(instanceManager.findClusterHealthCheckInstance(Mockito.anyString())).thenReturn(clusterInstance);
        Mockito.when(clusterInstance.getHealthCheckActions()).thenReturn(Collections.singletonList(action));
        Mockito.when(clusterInstance.toString()).thenReturn(clusterName);

    }

    @Test
    public void getHealthCheckInstanceTest() {
        String raw = healthController.getHealthCheckInstance("127.0.0.1", 6379);
        Assert.assertEquals(expectedResult, raw);
    }

    @Test
    public void getClusterHealthCheckInstance() {
        String raw = healthController.getClusterHealthCheckInstance(clusterName);
        Assert.assertEquals(expectedClusterResult, raw);
    }

}

