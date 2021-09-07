package com.ctrip.framework.xpipe.redis.utils;

import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;

import static com.ctrip.framework.xpipe.redis.instrument.ProxyAgentTool.HotspotVMName;
import static com.ctrip.framework.xpipe.redis.instrument.ProxyAgentTool.VirtualMachineClassName;

/**
 * @Author limingdong
 * @create 2021/4/28
 */
public class ToolsTest {

    @Test
    public void testNoClass() throws MalformedURLException, ClassNotFoundException {
        String pid = Tools.currentPID();
        Assert.assertTrue(Integer.parseInt(pid) > 0);
        Class clazz = Tools.loadJDKToolClass(HotspotVMName);
        Assert.assertNotNull(clazz);
    }
}