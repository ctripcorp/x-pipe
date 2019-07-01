package com.ctrip.xpipe.service.fireman;

import com.ctrip.framework.fireman.spi.TemporaryDependency;
import com.ctrip.framework.fireman.util.ServiceUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class XPipeTemporaryDependencyTest {

    private TemporaryDependency dependency;

    @Before
    public void beforeXPipeTemporaryDependencyTest() {
        dependency = ServiceUtil.getService(TemporaryDependency.class);
    }

    @Test
    public void testInstaniate() {
        Assert.assertNotNull(dependency);
        Assert.assertTrue(dependency instanceof XPipeTemporaryDependency);
    }

    @Test
    public void getMaster() {
        Assert.assertEquals(XPipeFiremanDependency.Environment.getInstance().getMasterNode(), dependency.getMaster());
    }

}