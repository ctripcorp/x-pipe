package com.ctrip.xpipe.redis.integratedtest.console.config;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

import static com.ctrip.xpipe.foundation.DefaultFoundationService.HOST_NAME_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFoundationServiceHostNameTest {

    @Test
    public void testGetHostNameNotEmpty() {
        ApplicationContext context = mock(ApplicationContext.class);
        Environment environment = mock(Environment.class);
        when(context.getEnvironment()).thenReturn(environment);
        when(environment.getProperty(HOST_NAME_KEY, "localhost")).thenReturn("ci-host-01");

        TestFoundationService service = new TestFoundationService();
        service.setApplicationContext(context);
        Assert.assertEquals("ci-host-01", service.getHostName());
        Assert.assertFalse(service.getHostName().isEmpty());
    }
}
