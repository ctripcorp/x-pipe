package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author ljin
 */
public class MetaUtilsTest extends AbstractTest {

    @Test
    public void testSameKeeperByIp() {
        KeeperMeta keeper1 = new KeeperMeta().setIp("192.168.1.1").setPort(6380);
        KeeperMeta keeper2 = new KeeperMeta().setIp("192.168.1.1").setPort(6380);
        
        Assert.assertTrue(MetaUtils.same(keeper1, keeper2));
    }

    @Test
    public void testSameKeeperByIpDifferentPort() {
        KeeperMeta keeper1 = new KeeperMeta().setIp("192.168.1.1").setPort(6380);
        KeeperMeta keeper2 = new KeeperMeta().setIp("192.168.1.1").setPort(6381);
        
        Assert.assertFalse(MetaUtils.same(keeper1, keeper2));
    }

    @Test
    public void testSameKeeperByHostname() {
        // 容器形态: IP为0.0.0.0, 使用hostname比对
        KeeperMeta keeper1 = new KeeperMeta().setIp("0.0.0.0").setPort(6380).setHostname("pod-a-0");
        KeeperMeta keeper2 = new KeeperMeta().setIp("0.0.0.0").setPort(6380).setHostname("pod-a-0");
        
        Assert.assertTrue(MetaUtils.same(keeper1, keeper2));
    }

    @Test
    public void testSameKeeperByHostnameDifferentPort() {
        KeeperMeta keeper1 = new KeeperMeta().setIp("0.0.0.0").setPort(6380).setHostname("pod-a-0");
        KeeperMeta keeper2 = new KeeperMeta().setIp("0.0.0.0").setPort(6381).setHostname("pod-a-0");
        
        Assert.assertFalse(MetaUtils.same(keeper1, keeper2));
    }

    @Test
    public void testSameKeeperByHostnameDifferentHostname() {
        KeeperMeta keeper1 = new KeeperMeta().setIp("0.0.0.0").setPort(6380).setHostname("pod-a-0");
        KeeperMeta keeper2 = new KeeperMeta().setIp("0.0.0.0").setPort(6380).setHostname("pod-b-0");
        
        Assert.assertFalse(MetaUtils.same(keeper1, keeper2));
    }

    @Test
    public void testSameKeeperMixedIpAndHostname() {
        // 物理机keeper (有IP) vs 容器keeper (有hostname) - 应该不相等
        KeeperMeta keeper1 = new KeeperMeta().setIp("192.168.1.1").setPort(6380);
        KeeperMeta keeper2 = new KeeperMeta().setIp("0.0.0.0").setPort(6380).setHostname("pod-a-0");
        
        Assert.assertFalse(MetaUtils.same(keeper1, keeper2));
    }

    @Test
    public void testSameKeeperEmptyIpWithHostname() {
        // IP为空或null时,应该用hostname比对
        KeeperMeta keeper1 = new KeeperMeta().setIp("").setPort(6380).setHostname("pod-a-0");
        KeeperMeta keeper2 = new KeeperMeta().setIp("").setPort(6380).setHostname("pod-a-0");
        
        Assert.assertTrue(MetaUtils.same(keeper1, keeper2));
    }

    @Test
    public void testSameKeeperNullIpWithHostname() {
        KeeperMeta keeper1 = new KeeperMeta().setIp(null).setPort(6380).setHostname("pod-a-0");
        KeeperMeta keeper2 = new KeeperMeta().setIp(null).setPort(6380).setHostname("pod-a-0");
        
        Assert.assertTrue(MetaUtils.same(keeper1, keeper2));
    }
}
