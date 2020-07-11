package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.healthcheck.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class HealthControllerTest extends AbstractConsoleTest {

    @InjectMocks
    private HealthController healthController;

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

    private String instanceName = "test-instance";
    private String actionName = "test-action";
    private String listenerName = "test-listener";
    private String controllerName = "test-controller";
    private String expectedResult = String.format("{\"info\":\"%s\",\"actions\":[{\"name\":\"%s\",\"listeners\":[\"%s\"],\"controllers\":[\"%s\"]}]}",
            instanceName, actionName, listenerName, controllerName);

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
    }

    @Test
    public void getHealthCheckInstanceTest() {
        String raw = healthController.getHealthCheckInstance("127.0.0.1", 6379);
        Assert.assertEquals(expectedResult, raw);
    }

}
