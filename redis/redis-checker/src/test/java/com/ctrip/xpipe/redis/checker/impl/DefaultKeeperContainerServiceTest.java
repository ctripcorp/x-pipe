package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

/**
 * Task 8: console 直连容器寻址 + 对外 API 反查
 * 测试 DefaultKeeperContainerService 通过 hostname 解析 IP 的能力
 */
public class DefaultKeeperContainerServiceTest extends AbstractCheckerTest {

    @Mock
    private KeeperContainerAddressService addressService;

    private DefaultKeeperContainerService keeperContainerService;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        keeperContainerService = new DefaultKeeperContainerService();
        keeperContainerService.setKeeperContainerAddressService(addressService);
    }

    @Test
    public void testResolveByHostname() {
        // 容器形态：addressService 能够解析 hostname -> IP
        when(addressService.getIpByHostname("pod-a-0")).thenReturn("10.0.0.5");
        
        // 调用 resolveAddress 时，hostname 应该被解析为真实 IP
        HostPort resolved = keeperContainerService.resolveAddress("pod-a-0", 8080);
        
        Assert.assertEquals(new HostPort("10.0.0.5", 8080), resolved);
    }

    @Test
    public void testResolveByRealIp() {
        // 物理机形态：已有真实 IP，直接返回
        HostPort resolved = keeperContainerService.resolveAddress("192.168.1.100", 8080);
        
        Assert.assertEquals(new HostPort("192.168.1.100", 8080), resolved);
    }

    @Test(expected = IllegalStateException.class)
    public void testResolveByEmptyOrPlaceholderIp() {
        // 空串场景：应该抛出异常
        keeperContainerService.resolveAddress("", 8080);
    }
    
    @Test(expected = IllegalStateException.class)
    public void testResolveByPlaceholderIp() {
        // 0.0.0.0 占位符场景（容器登记时的默认值），需要通过 hostname 反查
        // 但 0.0.0.0 不是 hostname，会尝试用 addressService 解析，结果为 null，抛异常
        when(addressService.getIpByHostname("0.0.0.0")).thenReturn(null);
        keeperContainerService.resolveAddress("0.0.0.0", 8080);
    }

    @Test
    public void testHostnameNotResolved() {
        // hostname 未注册或已下线，解析返回 null
        when(addressService.getIpByHostname("pod-offline")).thenReturn(null);
        
        // 应该抛出 IllegalStateException
        try {
            keeperContainerService.resolveAddress("pod-offline", 8080);
            Assert.fail("should throw IllegalStateException");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("pod-offline"));
        }
    }
}
