package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.HostNameVerification;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestOperations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 管理面请求必须打到 KeeperContainer 的 HTTP 端口（8080），而不是 Keeper 实例自身的 redis 端口 ——
 * 后者不说 HTTP，请求只会等到超时。
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultKeeperContainerServiceTest {

    private static final String DC = "jq";
    private static final String KEEPER_IP = "10.61.164.37";
    /** Keeper 实例自身的 redis 端口，不是管理面端口 */
    private static final int KEEPER_REDIS_PORT = 6408;
    private static final Long REPL_ID = 137348L;

    @Mock private RestOperations restTemplate;
    @Mock private CrossDcKeeperContainerAddressService addressService;

    private DefaultKeeperContainerService service;

    @Before
    public void setUp() {
        service = new DefaultKeeperContainerService();
        ReflectionTestUtils.setField(service, "restTemplate", restTemplate);
        ReflectionTestUtils.setField(service, "keeperContainerAddressService", addressService);
    }

    @Test
    public void testReleaseRdbUsesContainerManagementPort() {
        service.releaseRdb(DC, KEEPER_IP, null, KEEPER_REDIS_PORT, REPL_ID);

        ArgumentCaptor<String> url = ArgumentCaptor.forClass(String.class);
        verify(restTemplate).exchange(url.capture(), eq(HttpMethod.DELETE), any(HttpEntity.class), eq(Void.class));
        Assert.assertEquals("http://" + KEEPER_IP + ":" + KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT
                + "/keepers/rdb/release", url.getValue());
    }

    @Test
    public void testResetKeeperUsesContainerManagementPort() {
        service.resetKeeper(DC, KEEPER_IP, null, REPL_ID);

        ArgumentCaptor<String> url = ArgumentCaptor.forClass(String.class);
        verify(restTemplate).exchange(url.capture(), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class));
        Assert.assertEquals("http://" + KEEPER_IP + ":" + KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT
                + "/keepers/election/reset", url.getValue());
    }

    /** 容器形态按 hostname 反查地址，并携带自校验头。 */
    @Test
    public void testReleaseRdbResolvesHostname() {
        when(addressService.getIpByHostname(DC, "pod-a")).thenReturn("10.0.0.9");

        service.releaseRdb(DC, "0.0.0.0", "pod-a", KEEPER_REDIS_PORT, REPL_ID);

        ArgumentCaptor<String> url = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<HttpEntity> entity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(url.capture(), eq(HttpMethod.DELETE), entity.capture(), eq(Void.class));
        Assert.assertEquals("http://10.0.0.9:" + KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT
                + "/keepers/rdb/release", url.getValue());
        Assert.assertEquals("pod-a",
                entity.getValue().getHeaders().getFirst(HostNameVerification.HEADER_TARGET_HOST_NAME));
        Assert.assertEquals(REPL_ID, ((KeeperTransMeta) entity.getValue().getBody()).getReplId());
    }

    @Test(expected = IllegalStateException.class)
    public void testUnresolvableHostnameFailsFast() {
        when(addressService.getIpByHostname(DC, "pod-gone")).thenReturn(null);

        service.releaseRdb(DC, "0.0.0.0", "pod-gone", KEEPER_REDIS_PORT, REPL_ID);
    }
}
