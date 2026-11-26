package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.BeaconResp;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.console.impl.DefaultConsoleService;
import com.ctrip.xpipe.redis.console.controller.api.vo.RestResponse;
import com.ctrip.xpipe.redis.console.controller.api.vo.SentinelClusterBeaconRouteItem;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.core.beacon.BeaconRouteType;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.context.request.async.DeferredResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@RunWith(MockitoJUnitRunner.class)
public class BeaconRouteControllerTest extends AbstractConsoleTest {

    private static final String CLUSTER = "cluster-a";
    private static final String SHARD = "shard-a";
    private static final String SHARD_B = "shard-b";
    private static final String REMOTE_IDC = "remote-idc-for-test";
    private static final String SOURCE_IP = "1.1.1.1";
    private static final long ORG_ID = 3L;
    private static final String BEACON_SYSTEM = BeaconSystem.XPIPE_ONE_WAY.getSystemName();
    private static final int TIMEOUT_MILLI = 30000;
    private static final int FORWARD_EXTRA_MILLI = 4000;
    private static final HostPort MASTER = new HostPort("10.0.0.1", 6379);
    private static final HostPort SLAVE = new HostPort("10.0.0.2", 6379);
    private static final HostPort MASTER_B = new HostPort("10.0.1.1", 6379);
    private static final HostPort SLAVE_B = new HostPort("10.0.1.2", 6379);

    @InjectMocks
    private BeaconRouteController controller;

    @Mock
    private MonitorManager monitorManager;

    @Mock
    private MonitorService monitorService;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ShardService shardService;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ConsoleCommonConfig consoleCommonConfig;

    @Mock
    private ConsoleServiceManager consoleServiceManager;

    private String currentIdc;

    private MockHttpServletRequest request;

    @Before
    public void beforeBeaconRouteControllerTest() {
        ReflectionTestUtils.setField(controller, "executor", MoreExecutors.newDirectExecutorService());
        currentIdc = FoundationService.DEFAULT.getDataCenter();
        request = new MockHttpServletRequest();
        request.addHeader("X-FORWARDED-FOR", SOURCE_IP);
    }

    @Test
    public void shouldReturnSentinelRouteForClusterAndOrg() {
        Mockito.when(clusterService.find(any())).thenReturn(new ClusterTbl().setClusterName("cluster-a"));
        Mockito.when(monitorManager.get(eq(1L), eq("cluster-a"), Mockito.isNull(), eq(BeaconRouteType.SENTINEL)))
                .thenReturn(monitorService);
        Mockito.when(monitorService.getName()).thenReturn("beacon-a");
        Mockito.when(monitorService.getHost()).thenReturn("http://beacon-a");

        Map<String, String> result = controller.getSentinelBeaconRoute("cluster-a", 1L, null);

        Assert.assertEquals("beacon-a", result.get("beaconName"));
        Assert.assertEquals("http://beacon-a", result.get("beaconHost"));
        Assert.assertEquals("SENTINEL", result.get("routeType"));
    }

    @Test
    public void shouldReturnManagedClustersByBeacon() {
        Map<Long, List<MonitorService>> services = new HashMap<>();
        services.put(1L, Collections.singletonList(monitorService));
        Mockito.when(monitorManager.getAllServices(BeaconRouteType.SENTINEL)).thenReturn(services);
        Mockito.when(monitorService.getName()).thenReturn("beacon-a");
        Mockito.when(monitorService.getHost()).thenReturn("http://beacon-a");
        Mockito.when(monitorService.fetchAllClusters(BeaconSystem.XPIPE_ONE_WAY.getSystemName()))
                .thenReturn(Collections.singleton("cluster-a"));

        Map<String, Set<String>> result = controller.getSentinelBeaconClusters(null, null);

        Assert.assertEquals(Collections.singleton("cluster-a"), result.get("beacon-a@http://beacon-a"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterInCurrentIdcShouldForceFailoverByMonitorService() {
        mockLocalSwitchContext();
        mockShardRedises();
        mockShardBRedises();
        mockBeaconRoute();
        Map<String, HostPort> forced = new LinkedHashMap<>();
        forced.put(SHARD, SLAVE);
        forced.put(SHARD_B, SLAVE_B);
        Map<String, String> beaconStates = new LinkedHashMap<>();
        beaconStates.put(SHARD, "SUCCESS");
        beaconStates.put(SHARD_B, "SUCCESS");
        Mockito.when(monitorService.forceFailoverShardMasters(BEACON_SYSTEM, CLUSTER, forced, TIMEOUT_MILLI))
                .thenReturn(new BeaconResp<>(beaconStates));
        Map<String, String> body = new LinkedHashMap<>();
        body.put(SHARD, SLAVE.toString());
        body.put(SHARD_B, SLAVE_B.toString());

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                currentIdc, body);

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_SUCCESS, response.getCode());
        Assert.assertEquals("success", response.getMsg());
        Assert.assertEquals(beaconStates, response.getData());
        Assert.assertEquals((long) TIMEOUT_MILLI + FORWARD_EXTRA_MILLI, ReflectionTestUtils.getField(result, "timeoutValue"));
        Mockito.verify(monitorService).forceFailoverShardMasters(BEACON_SYSTEM, CLUSTER, forced, TIMEOUT_MILLI);
        Mockito.verifyNoInteractions(consoleServiceManager);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterShouldSendAllShardsToBeaconWhenSomeTargetIsNotMaster() {
        mockLocalSwitchContext();
        mockShardRedises();
        mockShardBRedises();
        mockBeaconRoute();
        // shard-a target is already the master, shard-b needs to switch
        Map<String, HostPort> forced = new LinkedHashMap<>();
        forced.put(SHARD, MASTER);
        forced.put(SHARD_B, SLAVE_B);
        Map<String, String> beaconStates = new LinkedHashMap<>();
        beaconStates.put(SHARD, "SUCCESS");
        beaconStates.put(SHARD_B, "SUCCESS");
        Mockito.when(monitorService.forceFailoverShardMasters(BEACON_SYSTEM, CLUSTER, forced, TIMEOUT_MILLI))
                .thenReturn(new BeaconResp<>(beaconStates));
        Map<String, String> body = new LinkedHashMap<>();
        body.put(SHARD, MASTER.toString());
        body.put(SHARD_B, SLAVE_B.toString());

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                currentIdc, body);

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_SUCCESS, response.getCode());
        Assert.assertEquals(beaconStates, response.getData());
        Mockito.verify(monitorService).forceFailoverShardMasters(BEACON_SYSTEM, CLUSTER, forced, TIMEOUT_MILLI);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterShouldIgnoreIdcCase() {
        mockLocalSwitchContext();
        mockShardRedises();

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                currentIdc.toUpperCase(), shardMasters(SHARD, MASTER));

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_SUCCESS, response.getCode());
        Mockito.verifyNoInteractions(consoleServiceManager);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterShouldSkipBeaconWhenTargetIsAlreadyMaster() {
        mockLocalSwitchContext();
        mockShardRedises();

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                currentIdc, shardMasters(SHARD, MASTER));

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_SUCCESS, response.getCode());
        Assert.assertTrue(response.getMsg().contains("already"));
        Mockito.verifyNoInteractions(monitorService, monitorManager);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterShouldRejectTargetNotInShard() {
        mockLocalSwitchContext();
        mockShardRedises();

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                currentIdc, shardMasters(SHARD, new HostPort("10.0.0.3", 6379)));

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_FAIL, response.getCode());
        Assert.assertTrue(response.getMsg().contains("is not in shard"));
        Mockito.verifyNoInteractions(monitorService);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterShouldRejectEmptyShardMasters() {
        mockLocalSwitchContext();

        for (Map<String, String> body : Arrays.asList(null, Collections.<String, String>emptyMap())) {
            DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                    currentIdc, body);

            RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
            Assert.assertEquals(RestResponse.CODE_FAIL, response.getCode());
            Assert.assertTrue(response.getMsg().contains("shard masters must not be empty"));
        }
        Mockito.verifyNoInteractions(clusterService, monitorService);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterShouldRejectEmptyShardOrTarget() {
        mockLocalSwitchContext();

        for (Map<String, String> body : Arrays.asList(Collections.singletonMap("", SLAVE.toString()),
                Collections.singletonMap(SHARD, ""), Collections.<String, String>singletonMap(SHARD, null))) {
            DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                    currentIdc, body);

            RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
            Assert.assertEquals(RestResponse.CODE_FAIL, response.getCode());
            Assert.assertTrue(response.getMsg().contains("shard and target must not be empty"));
        }
        Mockito.verifyNoInteractions(clusterService, monitorService);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterShouldRejectInvalidTarget() {
        mockLocalSwitchContext();

        for (String target : new String[]{"10.0.0.2", "10.0.0.2:abc", ":6379", "10.0.0.2:0", "10.0.0.2:-1",
                "10.0.0.2:65536"}) {
            DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                    currentIdc, Collections.singletonMap(SHARD, target));

            RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
            Assert.assertEquals(RestResponse.CODE_FAIL, response.getCode());
            Assert.assertTrue(response.getMsg().contains("invalid target of shard " + SHARD));
        }
        Mockito.verifyNoInteractions(clusterService, monitorService);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterShouldPassThroughBeaconFailure() {
        mockLocalSwitchContext();
        mockShardRedises();
        mockBeaconRoute();
        Map<String, String> states = Collections.singletonMap(SHARD, "MIGRATING");
        Mockito.when(monitorService.forceFailoverShardMasters(BEACON_SYSTEM, CLUSTER,
                        Collections.singletonMap(SHARD, SLAVE), TIMEOUT_MILLI))
                .thenReturn(new BeaconResp<>(5002, "force failover still running after 20000 ms, fail reasons: {}", states));

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                currentIdc, shardMasters(SHARD, SLAVE));

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(5002, response.getCode());
        Assert.assertEquals("force failover still running after 20000 ms, fail reasons: {}", response.getMsg());
        Assert.assertSame(states, response.getData());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterShouldReturnErrorWhenBeaconUnreachable() {
        mockLocalSwitchContext();
        mockShardRedises();
        mockBeaconRoute();
        Mockito.when(monitorService.forceFailoverShardMasters(BEACON_SYSTEM, CLUSTER,
                        Collections.singletonMap(SHARD, SLAVE), TIMEOUT_MILLI))
                .thenThrow(new ResourceAccessException("Read timed out"));

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                currentIdc, shardMasters(SHARD, SLAVE));

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_FAIL, response.getCode());
        Assert.assertEquals("Read timed out", response.getMsg());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterShouldReturnErrorWhenNoBeaconRoute() {
        mockLocalSwitchContext();
        mockShardRedises();
        SentinelClusterBeaconRouteItem unmanaged = new SentinelClusterBeaconRouteItem();
        unmanaged.setManagedByBeacon(false);
        unmanaged.setBeaconHost("http://beacon-a");
        Mockito.when(monitorManager.getSentinelClusterRoutes(CLUSTER)).thenReturn(Collections.singletonList(unmanaged));

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                currentIdc, shardMasters(SHARD, SLAVE));

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_FAIL, response.getCode());
        Assert.assertTrue(response.getMsg().contains("sentinel beacon route not found"));
        Mockito.verifyNoInteractions(monitorService);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterShouldReturnErrorWhenMonitorServiceNotFound() {
        mockLocalSwitchContext();
        mockShardRedises();
        mockBeaconRouteItem();
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER, null, BeaconRouteType.SENTINEL)).thenReturn(null);

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                currentIdc, shardMasters(SHARD, SLAVE));

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_FAIL, response.getCode());
        Assert.assertTrue(response.getMsg().contains("sentinel beacon service not found"));
    }

    @Test
    public void switchMasterInRemoteIdcShouldForwardToConsoleOfThatIdc() {
        mockForwardContext();
        Map<String, String> body = shardMasters(SHARD, SLAVE);
        RestResponse<Map<String, String>> remoteResponse = RestResponse.success(Collections.singletonMap(SHARD, "SUCCESS"));
        Mockito.when(consoleServiceManager.postForSwitchBeaconMaster(REMOTE_IDC, CLUSTER, body, SOURCE_IP))
                .thenReturn(remoteResponse);

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                REMOTE_IDC, body);

        Assert.assertSame(remoteResponse, result.getResult());
        Assert.assertEquals((long) TIMEOUT_MILLI + FORWARD_EXTRA_MILLI + FORWARD_EXTRA_MILLI,
                ReflectionTestUtils.getField(result, "timeoutValue"));
        Mockito.verifyNoInteractions(clusterService, shardService, metaCache, monitorManager, monitorService);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterInRemoteIdcShouldValidateBeforeForward() {
        mockForwardContext();

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                REMOTE_IDC, Collections.singletonMap(SHARD, SLAVE.getHost()));

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_FAIL, response.getCode());
        Mockito.verify(consoleServiceManager, Mockito.never()).postForSwitchBeaconMaster(any(), any(), any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterInRemoteIdcShouldFailWithoutConsoleServiceManager() {
        ReflectionTestUtils.setField(controller, "consoleServiceManager", null);
        Mockito.when(consoleCommonConfig.getBeaconFailoverMasterSwitchForwardExtraMilli()).thenReturn(FORWARD_EXTRA_MILLI);

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                REMOTE_IDC, shardMasters(SHARD, SLAVE));

        Assert.assertEquals((long) DefaultConsoleService.DEFAULT_BEACON_MASTER_SWITCH_READ_TIMEOUT_MILLI + FORWARD_EXTRA_MILLI,
                ReflectionTestUtils.getField(result, "timeoutValue"));
        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_FAIL, response.getCode());
        Assert.assertTrue(response.getMsg().contains("console service manager unavailable"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void switchMasterInRemoteIdcShouldReturnErrorWhenForwardFail() {
        mockForwardContext();
        Mockito.when(consoleServiceManager.postForSwitchBeaconMaster(any(), any(), any(), any()))
                .thenThrow(new ResourceAccessException("connect refused"));

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMaster(request, CLUSTER,
                REMOTE_IDC, shardMasters(SHARD, SLAVE));

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_FAIL, response.getCode());
        Assert.assertEquals("connect refused", response.getMsg());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void localEndpointShouldRejectNonCurrentIdc() {
        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMasterLocal(request, CLUSTER,
                REMOTE_IDC, shardMasters(SHARD, SLAVE));

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_FAIL, response.getCode());
        Assert.assertTrue(response.getMsg().contains("is not current idc"));
        Mockito.verifyNoInteractions(consoleServiceManager, clusterService, monitorService);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void localEndpointShouldSwitchInCurrentIdc() {
        mockLocalSwitchContext();
        mockShardRedises();

        DeferredResult<RestResponse<Map<String, String>>> result = controller.switchBeaconMasterLocal(request, CLUSTER,
                currentIdc, shardMasters(SHARD, MASTER));

        RestResponse<Map<String, String>> response = (RestResponse<Map<String, String>>) result.getResult();
        Assert.assertEquals(RestResponse.CODE_SUCCESS, response.getCode());
        Mockito.verifyNoInteractions(consoleServiceManager);
    }

    private static Map<String, String> shardMasters(String shard, HostPort target) {
        return Collections.singletonMap(shard, target.toString());
    }

    private void mockForwardContext() {
        Mockito.when(consoleServiceManager.getBeaconMasterSwitchForwardTimeoutMilli())
                .thenReturn(TIMEOUT_MILLI + FORWARD_EXTRA_MILLI);
        Mockito.when(consoleCommonConfig.getBeaconFailoverMasterSwitchForwardExtraMilli()).thenReturn(FORWARD_EXTRA_MILLI);
    }

    private void mockLocalSwitchContext() {
        Mockito.when(consoleCommonConfig.getBeaconFailoverMasterSwitchTimeoutMilli()).thenReturn(TIMEOUT_MILLI);
        Mockito.when(consoleCommonConfig.getBeaconFailoverMasterSwitchForwardExtraMilli()).thenReturn(FORWARD_EXTRA_MILLI);
    }

    private void mockShardRedises() {
        Mockito.when(clusterService.find(CLUSTER)).thenReturn(new ClusterTbl().setClusterName(CLUSTER));
        Mockito.when(shardService.find(CLUSTER, SHARD)).thenReturn(new ShardTbl().setShardName(SHARD));
        Mockito.when(metaCache.getRedisOfDcClusterShard(currentIdc, CLUSTER, SHARD)).thenReturn(Arrays.asList(
                new RedisMeta().setIp(MASTER.getHost()).setPort(MASTER.getPort()),
                new RedisMeta().setIp(SLAVE.getHost()).setPort(SLAVE.getPort()).setMaster(MASTER.toString())));
    }

    private void mockShardBRedises() {
        Mockito.when(shardService.find(CLUSTER, SHARD_B)).thenReturn(new ShardTbl().setShardName(SHARD_B));
        Mockito.when(metaCache.getRedisOfDcClusterShard(currentIdc, CLUSTER, SHARD_B)).thenReturn(Arrays.asList(
                new RedisMeta().setIp(MASTER_B.getHost()).setPort(MASTER_B.getPort()),
                new RedisMeta().setIp(SLAVE_B.getHost()).setPort(SLAVE_B.getPort()).setMaster(MASTER_B.toString())));
    }

    private void mockBeaconRoute() {
        mockBeaconRouteItem();
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER, null, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
    }

    private void mockBeaconRouteItem() {
        SentinelClusterBeaconRouteItem route = new SentinelClusterBeaconRouteItem();
        route.setSystem(BEACON_SYSTEM);
        route.setOrgId(ORG_ID);
        route.setManagedByBeacon(true);
        route.setBeaconName("beacon-a");
        route.setBeaconHost("http://beacon-a");
        Mockito.when(monitorManager.getSentinelClusterRoutes(CLUSTER)).thenReturn(Collections.singletonList(route));
    }
}
