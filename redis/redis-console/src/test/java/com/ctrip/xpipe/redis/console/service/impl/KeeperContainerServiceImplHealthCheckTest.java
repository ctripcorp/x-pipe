package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestOperations;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * {@link KeeperContainerServiceImpl#keeperContainerIsHealthy} 双轨探测（spec §8.7）。
 */
@RunWith(MockitoJUnitRunner.class)
public class KeeperContainerServiceImplHealthCheckTest {

    private static final String DC = "jq";
    private static final String HOSTNAME = "kc-pod-1";
    private static final String POD_IP = "10.0.0.5";
    private static final String PHYSICAL_IP = "127.0.0.1";
    private static final int PORT = 8080;

    @InjectMocks
    private KeeperContainerServiceImpl keeperContainerService;

    @Mock
    private CrossDcKeeperContainerAddressService keeperContainerAddressService;

    @Mock
    private RestOperations restTemplate;

    @Before
    public void setUp() {
        keeperContainerService.setRestTemplate(restTemplate);
    }

    @Test
    public void testPhysicalHealthyProbesRegisteredIp() {
        when(restTemplate.getForObject(healthUrl(PHYSICAL_IP), Boolean.class)).thenReturn(true);

        Assert.assertTrue(keeperContainerService.keeperContainerIsHealthy(null, PHYSICAL_IP, PORT, DC));
        verifyNoInteractions(keeperContainerAddressService);
    }

    @Test
    public void testPhysicalUnhealthyReturnsFalse() {
        when(restTemplate.getForObject(healthUrl(PHYSICAL_IP), Boolean.class)).thenReturn(false);

        Assert.assertFalse(keeperContainerService.keeperContainerIsHealthy("", PHYSICAL_IP, PORT, DC));
        verifyNoInteractions(keeperContainerAddressService);
    }

    @Test
    public void testPhysicalEmptyIpReturnsUnhealthyInsteadOfNotFound() {
        when(restTemplate.getForObject(anyString(), eq(Boolean.class)))
                .thenThrow(new ResourceAccessException("connect fail"));

        Assert.assertFalse(keeperContainerService.keeperContainerIsHealthy(null, null, PORT, DC));
        verifyNoInteractions(keeperContainerAddressService);
    }

    @Test
    public void testContainerNotResolvedThrowsNotFound() {
        when(keeperContainerAddressService.getIpByHostname(DC, HOSTNAME)).thenReturn(null);

        try {
            keeperContainerService.keeperContainerIsHealthy(HOSTNAME, "0.0.0.0", PORT, DC);
            Assert.fail("expected not found");
        } catch (XpipeRuntimeException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(HOSTNAME));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("not found"));
        }
        verifyNoInteractions(restTemplate);
    }

    @Test
    public void testContainerResolvedProbesResolvedIp() {
        when(keeperContainerAddressService.getIpByHostname(DC, HOSTNAME)).thenReturn(POD_IP);
        when(restTemplate.getForObject(healthUrl(POD_IP), Boolean.class)).thenReturn(true);

        Assert.assertTrue(keeperContainerService.keeperContainerIsHealthy(HOSTNAME, "0.0.0.0", PORT, DC));
        verify(restTemplate).getForObject(healthUrl(POD_IP), Boolean.class);
    }

    @Test
    public void testContainerResolvedButUnhealthyReturnsFalse() {
        when(keeperContainerAddressService.getIpByHostname(DC, HOSTNAME)).thenReturn(POD_IP);
        when(restTemplate.getForObject(healthUrl(POD_IP), Boolean.class)).thenReturn(false);

        Assert.assertFalse(keeperContainerService.keeperContainerIsHealthy(HOSTNAME, null, PORT, DC));
    }

    private static String healthUrl(String ip) {
        return String.format("http://%s:%d/health", ip, PORT);
    }
}
