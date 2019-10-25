package com.ctrip.xpipe.service.fireman;

import com.ctrip.framework.fireman.spi.FiremanDependency;
import com.ctrip.framework.fireman.util.ServiceUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ServiceLoader;

import static org.junit.Assert.*;

public class XPipeFiremanDependencyTest {

    private FiremanDependency dependency;

    @Before
    public void beforeXPipeFiremanDependencyTest() {
        dependency = ServiceUtil.getService(FiremanDependency.class);
    }

    @Test
    public void getDatabaseDomainName() {
        Assert.assertNotNull(dependency);
        Assert.assertTrue(dependency instanceof XPipeFiremanDependency);
        Assert.assertEquals(XPipeFiremanDependency.Environment.getInstance().getDatabaseDomainName(), dependency.getDatabaseDomainName());
    }
}