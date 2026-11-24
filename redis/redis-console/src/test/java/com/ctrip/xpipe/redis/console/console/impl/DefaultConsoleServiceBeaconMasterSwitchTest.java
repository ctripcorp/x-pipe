package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.controller.api.vo.BeaconFailoverMasterSwitchRequest;
import com.ctrip.xpipe.redis.console.controller.api.vo.BeaconFailoverResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestOperations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@RunWith(MockitoJUnitRunner.class)
public class DefaultConsoleServiceBeaconMasterSwitchTest extends AbstractConsoleTest {

    private static final String URL = "http://console-remote:8080/api/beacon/failover/master/switch/local"
            + "?cluster={cluster}&shard={shard}&idc={idc}";

    @Mock
    private RestOperations restOperations;

    private DefaultConsoleService consoleService;

    @Before
    public void beforeDefaultConsoleServiceBeaconMasterSwitchTest() {
        consoleService = new DefaultConsoleService("console-remote:8080", 40000);
        ReflectionTestUtils.setField(consoleService, "beaconMasterSwitchRestTemplate", restOperations);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void shouldPostIpAndPortToLocalEndpointOfRemoteConsole() {
        BeaconFailoverResponse<Object> remoteResponse = new BeaconFailoverResponse<>("FIXED_OR_WAIT");
        Mockito.doReturn(ResponseEntity.ok(remoteResponse)).when(restOperations).exchange(eq(URL), eq(HttpMethod.POST),
                any(HttpEntity.class), any(ParameterizedTypeReference.class), eq("cluster-a"), eq("shard-a"), eq("REMOTE"));

        BeaconFailoverResponse<Object> response = consoleService.postForSwitchBeaconMaster("cluster-a", "shard-a", "REMOTE",
                "10.0.0.2", 6379, "1.1.1.1");

        Assert.assertSame(remoteResponse, response);
        ArgumentCaptor<HttpEntity> entityCaptor = ArgumentCaptor.forClass(HttpEntity.class);
        Mockito.verify(restOperations).exchange(eq(URL), eq(HttpMethod.POST), entityCaptor.capture(),
                any(ParameterizedTypeReference.class), eq("cluster-a"), eq("shard-a"), eq("REMOTE"));
        HttpEntity<BeaconFailoverMasterSwitchRequest> entity = entityCaptor.getValue();
        Assert.assertEquals("10.0.0.2", entity.getBody().getIp());
        Assert.assertEquals(Integer.valueOf(6379), entity.getBody().getPort());
        Assert.assertEquals("1.1.1.1", entity.getHeaders().getFirst("X-FORWARDED-FOR"));
        Assert.assertEquals(MediaType.APPLICATION_JSON, entity.getHeaders().getContentType());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void shouldNotSetForwardedHeaderWhenSourceIpEmpty() {
        Mockito.doReturn(ResponseEntity.ok(new BeaconFailoverResponse<>())).when(restOperations).exchange(eq(URL),
                eq(HttpMethod.POST), any(HttpEntity.class), any(ParameterizedTypeReference.class),
                eq("cluster-a"), eq("shard-a"), eq("REMOTE"));

        consoleService.postForSwitchBeaconMaster("cluster-a", "shard-a", "REMOTE", "10.0.0.2", 6379, null);

        ArgumentCaptor<HttpEntity> entityCaptor = ArgumentCaptor.forClass(HttpEntity.class);
        Mockito.verify(restOperations).exchange(eq(URL), eq(HttpMethod.POST), entityCaptor.capture(),
                any(ParameterizedTypeReference.class), eq("cluster-a"), eq("shard-a"), eq("REMOTE"));
        Assert.assertNull(entityCaptor.getValue().getHeaders().getFirst("X-FORWARDED-FOR"));
    }

    @Test
    public void shouldUseDedicatedRestTemplateForMasterSwitch() {
        DefaultConsoleService service = new DefaultConsoleService("console-remote:8080", 40000);
        Object switchTemplate = ReflectionTestUtils.getField(service, "beaconMasterSwitchRestTemplate");

        Assert.assertNotNull(switchTemplate);
        Assert.assertNotSame(ReflectionTestUtils.getField(service, "sentinelMigrationRestTemplate"), switchTemplate);
        Assert.assertNotSame(ReflectionTestUtils.getField(service, "restTemplate"), switchTemplate);
    }
}
