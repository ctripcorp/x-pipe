package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.BeaconResp;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.console.impl.DefaultConsoleService;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.vo.*;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.notifier.SentinelBeaconClusterMonitorNotifier;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.core.beacon.BeaconRouteType;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import java.util.*;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX + "/beacon")
public class BeaconRouteController extends AbstractConsoleController {

    @Autowired
    private MonitorManager monitorManager;

    @Autowired
    private BeaconManager beaconManager;

    @Autowired(required = false)
    private ConsoleServiceManager consoleServiceManager;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private MetaCache metaCache;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executor;

    @Autowired
    private ConsoleCommonConfig consoleCommonConfig;
    @Autowired(required = false)
    private SentinelBeaconClusterMonitorNotifier sentinelBeaconClusterMonitorNotifier;

    @PostMapping("/failover/master/switch")
    public DeferredResult<RestResponse<Map<String, String>>> switchBeaconMaster(HttpServletRequest request,
                                                                              @RequestParam("cluster") String cluster,
                                                                              @RequestParam("idc") String idc,
                                                                              @RequestBody Map<String, String> shardMasters) {
        String forwardedFor = request.getHeader("X-FORWARDED-FOR");
        String sourceIp = StringUtil.isEmpty(forwardedFor) ? request.getRemoteAddr() : forwardedFor;
        if (!StringUtil.isEmpty(idc) && idc.equalsIgnoreCase(FoundationService.DEFAULT.getDataCenter())) {
            return submitLocalMasterSwitch(sourceIp, cluster, idc, shardMasters);
        }
        int forwardTimeoutMilli = consoleServiceManager == null
                ? DefaultConsoleService.DEFAULT_BEACON_MASTER_SWITCH_READ_TIMEOUT_MILLI
                : consoleServiceManager.getBeaconMasterSwitchForwardTimeoutMilli();
        return submitMasterSwitch(forwardTimeoutMilli,
                () -> forwardMasterSwitch(idc, cluster, shardMasters, sourceIp));
    }

    @PostMapping("/failover/master/switch/local")
    public DeferredResult<RestResponse<Map<String, String>>> switchBeaconMasterLocal(HttpServletRequest request,
                                                                                   @RequestParam("cluster") String cluster,
                                                                                   @RequestParam("idc") String idc,
                                                                                   @RequestBody Map<String, String> shardMasters) {
        String currentIdc = FoundationService.DEFAULT.getDataCenter();
        if (StringUtil.isEmpty(idc) || !idc.equalsIgnoreCase(currentIdc)) {
            DeferredResult<RestResponse<Map<String, String>>> deferredResult = new DeferredResult<>();
            deferredResult.setResult(RestResponse.fail(String.format("idc %s is not current idc %s", idc, currentIdc)));
            return deferredResult;
        }
        String forwardedFor = request.getHeader("X-FORWARDED-FOR");
        String sourceIp = StringUtil.isEmpty(forwardedFor) ? request.getRemoteAddr() : forwardedFor;
        return submitLocalMasterSwitch(sourceIp, cluster, idc, shardMasters);
    }

    private DeferredResult<RestResponse<Map<String, String>>> submitLocalMasterSwitch(String sourceIp, String cluster,
                                                                                      String idc,
                                                                                      Map<String, String> shardMasters) {
        int timeoutMilli = consoleCommonConfig.getBeaconFailoverMasterSwitchTimeoutMilli();
        return submitMasterSwitch(timeoutMilli,
                () -> executeMasterSwitch(timeoutMilli, cluster, idc, shardMasters, sourceIp));
    }

    private DeferredResult<RestResponse<Map<String, String>>> submitMasterSwitch(int httpTimeoutMilli,
                                                                                 Supplier<RestResponse<Map<String, String>>> task) {
        DeferredResult<RestResponse<Map<String, String>>> deferredResult =
                new DeferredResult<>((long) httpTimeoutMilli
                        + consoleCommonConfig.getBeaconFailoverMasterSwitchForwardExtraMilli());
        deferredResult.onTimeout(() -> deferredResult.setResult(
                RestResponse.fail("beacon master switch timeout, result unknown")));
        executor.execute(() -> deferredResult.setResult(task.get()));
        return deferredResult;
    }

    private Map<String, HostPort> parseShardMasters(String cluster, String idc, Map<String, String> shardMasters) {
        if (StringUtil.isEmpty(cluster) || StringUtil.isEmpty(idc)) {
            throw new IllegalArgumentException("cluster and idc must not be empty");
        }
        if (shardMasters == null || shardMasters.isEmpty()) {
            throw new IllegalArgumentException("shard masters must not be empty");
        }
        Map<String, HostPort> targets = new LinkedHashMap<>();
        shardMasters.forEach((shard, target) -> targets.put(shard, parseTarget(shard, target)));
        return targets;
    }

    private HostPort parseTarget(String shard, String target) {
        if (StringUtil.isEmpty(shard) || StringUtil.isEmpty(target)) {
            throw new IllegalArgumentException("shard and target must not be empty");
        }
        try {
            HostPort hostPort = HostPort.fromString(target.trim());
            if (!StringUtil.isEmpty(hostPort.getHost()) && hostPort.getPort() > 0 && hostPort.getPort() <= 65535) {
                return hostPort;
            }
        } catch (RuntimeException ignore) {
            // not ip:port, reported below
        }
        throw new IllegalArgumentException("invalid target of shard " + shard + ": " + target);
    }

    private RestResponse<Map<String, String>> forwardMasterSwitch(String idc, String cluster,
                                                                  Map<String, String> shardMasters, String sourceIp) {
        Transaction transaction = Cat.newTransaction("Beacon.Force.Failover", "MasterSwitchForward");
        transaction.addData("sourceIp", sourceIp);
        transaction.addData("idc", idc);
        transaction.addData("cluster", cluster);
        try {
            transaction.addData("shardMasters", parseShardMasters(cluster, idc, shardMasters));
            if (consoleServiceManager == null) {
                throw new IllegalStateException("console service manager unavailable, can not forward to idc " + idc);
            }
            RestResponse<Map<String, String>> response = consoleServiceManager.postForSwitchBeaconMaster(idc, cluster,
                    shardMasters, sourceIp);
            if (response == null) {
                throw new IllegalStateException("console of idc " + idc + " returned no response body");
            }
            transaction.addData("code", response.getCode());
            if (RestResponse.CODE_SUCCESS == response.getCode()) {
                transaction.setStatus(Transaction.SUCCESS);
            } else {
                transaction.setStatus(new IllegalStateException(response.getMsg()));
            }
            return response;
        } catch (Throwable th) {
            transaction.setStatus(th);
            return RestResponse.fail(th.getMessage());
        } finally {
            transaction.complete();
        }
    }

    private RestResponse<Map<String, String>> executeMasterSwitch(int timeoutMilli, String cluster, String idc,
                                                                  Map<String, String> shardMasters, String sourceIp) {
        Transaction transaction = Cat.newTransaction("Beacon.Force.Failover", "MasterSwitch");
        transaction.addData("sourceIp", sourceIp);
        transaction.addData("idc", idc);
        transaction.addData("cluster", cluster);
        try {
            Map<String, HostPort> targets = parseShardMasters(cluster, idc, shardMasters);
            transaction.addData("shardMasters", targets);
            ClusterTbl clusterTbl = clusterService.find(cluster);
            if (clusterTbl == null) {
                throw new IllegalArgumentException("cluster not found: " + cluster);
            }
            String clusterName = clusterTbl.getClusterName();

            Map<String, HostPort> forcedShardMasters = new LinkedHashMap<>();
            boolean allTargetsAreMaster = true;
            for (Map.Entry<String, HostPort> entry : targets.entrySet()) {
                ShardTbl shardTbl = shardService.find(clusterName, entry.getKey());
                if (shardTbl == null) {
                    throw new IllegalArgumentException("shard not found: " + entry.getKey());
                }
                String shardName = shardTbl.getShardName();
                HostPort target = entry.getValue();
                List<RedisMeta> redises = metaCache.getRedisOfDcClusterShard(
                        FoundationService.DEFAULT.getDataCenter(), clusterName, shardName);
                RedisMeta currentMaster = redises.stream().filter(RedisMeta::isMaster).findFirst()
                        .orElseThrow(() -> new IllegalStateException("current master not found in shard " + shardName));
                if (redises.stream().noneMatch(redis -> target.equals(new HostPort(redis.getIp(), redis.getPort())))) {
                    throw new IllegalArgumentException("target " + target + " is not in shard " + shardName + " of idc " + idc);
                }
                if (null != forcedShardMasters.put(shardName, target)) {
                    throw new IllegalArgumentException("duplicate shard: " + shardName);
                }
                allTargetsAreMaster &= target.equals(new HostPort(currentMaster.getIp(), currentMaster.getPort()));
            }
            // every target is already the master in xpipe meta, no need to call beacon
            if (allTargetsAreMaster) {
                transaction.setStatus(Transaction.SUCCESS);
                return new RestResponse<>(RestResponse.CODE_SUCCESS,
                        "current master is already the target: " + forcedShardMasters, null);
            }

            SentinelClusterBeaconRouteItem route = monitorManager.getSentinelClusterRoutes(clusterName)
                    .stream()
                    .filter(SentinelClusterBeaconRouteItem::isManagedByBeacon)
                    .filter(item -> !StringUtil.isEmpty(item.getBeaconHost()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("sentinel beacon route not found"));
            MonitorService monitorService = monitorManager.get(route.getOrgId(), clusterName,
                    null, BeaconRouteType.SENTINEL);
            if (monitorService == null) {
                throw new IllegalStateException("sentinel beacon service not found");
            }
            transaction.addData("beaconName", monitorService.getName());
            transaction.addData("beaconHost", monitorService.getHost());
            transaction.addData("beaconSystem", route.getSystem());

            // shards whose target is already master are sent too, beacon marks them SUCCESS in its first round
            BeaconResp<Map<String, String>> result = monitorService.forceFailoverShardMasters(route.getSystem(),
                    clusterName, forcedShardMasters, timeoutMilli);
            if (result == null) {
                throw new IllegalStateException("beacon returned no result");
            }
            transaction.addData("beaconCode", result.getCode());
            transaction.addData("beaconResult", result.getData());
            if (result.isSuccess()) {
                transaction.setStatus(Transaction.SUCCESS);
            } else {
                transaction.setStatus(new IllegalStateException(result.getMsg()));
            }
            return new RestResponse<>(result.getCode(), result.getMsg(), result.getData());
        } catch (Throwable th) {
            transaction.setStatus(th);
            return RestResponse.fail(th.getMessage());
        } finally {
            transaction.complete();
        }
    }

    @GetMapping("/sentinel/clusters")
    public Map<String, Set<String>> getSentinelBeaconClusters(@RequestParam(name = "system", required = false) String system,
                                                               @RequestParam(name = "routeType", required = false) String routeType) {
        BeaconRouteType selectedRouteType = parseRouteType(routeType);
        String systemName = (system == null || system.isEmpty()) ? BeaconSystem.XPIPE_ONE_WAY.getSystemName() : system;
        Map<Long, List<MonitorService>> services = monitorManager.getAllServices(selectedRouteType);
        if (services.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Set<String>> result = new LinkedHashMap<>();
        services.values().stream().flatMap(List::stream).forEach(service -> {
            Set<String> clusters = service.fetchAllClusters(systemName);
            String key = service.getName() + "@" + service.getHost();
            result.put(key, clusters);
        });
        return result;
    }

    @GetMapping("/sentinel/route")
    public Map<String, String> getSentinelBeaconRoute(@RequestParam("clusterName") String clusterName,
                                                       @RequestParam("orgId") long orgId,
                                                       @RequestParam(name = "routeType", required = false) String routeType) {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            return new HashMap<>();
        }

        BeaconRouteType selectedRouteType = parseRouteType(routeType);
        MonitorService service = monitorManager.get(orgId, clusterTbl.getClusterName(), null, selectedRouteType);
        if (service == null) {
            return Collections.emptyMap();
        }
        Map<String, String> route = new LinkedHashMap<>();
        route.put("clusterName", clusterName);
        route.put("orgId", String.valueOf(orgId));
        route.put("beaconName", service.getName());
        route.put("beaconHost", service.getHost());
        route.put("routeType", selectedRouteType.name());
        return route;
    }

    /**
     * Hash of local monitor cluster meta, same computation as {@link com.ctrip.xpipe.redis.console.migration.auto.DefaultBeaconManager#checkClusterHash}
     * local side ({@code MonitorClusterMeta#generateHashCodeForBeaconCheck(boolean)}).
     */
    @GetMapping("/sentinel/cluster/hash")
    public Map<String, Object> getClusterMetaHash(@RequestParam("clusterName") String clusterName,
                                                  @RequestParam("clusterType") String clusterType,
                                                  @RequestParam(name = "dc", required = false) String dc,
                                                  @RequestParam(name = "routeType", required = false) String routeType) {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            return new HashMap<>();
        }

        BeaconRouteType selectedRouteType = parseRouteType(routeType);
        ClusterType type = ClusterType.lookup(clusterType);
        String anchorDc = (dc == null || dc.isEmpty()) ? FoundationService.DEFAULT.getDataCenter() : dc;
        int metaHash = beaconManager.computeClusterMetaHash(clusterTbl.getClusterName(), anchorDc, type, selectedRouteType);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("clusterName", clusterName);
        result.put("clusterType", type.name());
        result.put("dc", anchorDc);
        result.put("routeType", selectedRouteType.name());
        result.put("metaHash", metaHash);
        return result;
    }

    @DeleteMapping("/sentinel/cluster/" + CLUSTER_NAME_PATH_VARIABLE)
    public RetMessage deleteClusterFromSentinelBeacon(@PathVariable("clusterName") String clusterName,
                                                      @RequestParam(name = "dc", required = false) String dc) {

        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            return RetMessage.createFailMessage("cluster_not_found");
        }

        long orgId = clusterTbl.getClusterOrgId();
        if (sentinelBeaconClusterMonitorNotifier == null) {
            return RetMessage.createFailMessage("sentinel_beacon_notifier_unavailable");
        }
        sentinelBeaconClusterMonitorNotifier.notifyClusterDelete(clusterTbl.getClusterName(), dc, orgId);

        return RetMessage.createSuccessMessage();
    }

    @GetMapping("/sentinel/usage")
    public Map<String, RestResponse<List<SentinelBeaconUsageItem>>> getSentinelBeaconUsage(
            @RequestParam(required = false, defaultValue = "xpipe") String system,
            @RequestParam(required = false, defaultValue = "false") boolean includeClusters) {
        if (consoleServiceManager == null) {
            String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();
            Map<String, RestResponse<List<SentinelBeaconUsageItem>>> result = new LinkedHashMap<>();
            result.put(currentDc, RestResponse.success(monitorManager.getSentinelBeaconUsage(system, includeClusters)));
            return result;
        }
        return consoleServiceManager.getAllConsoleSentinelBeaconUsage(system, includeClusters);
    }

    @GetMapping("/sentinel/usage/local")
    public List<SentinelBeaconUsageItem> getSentinelBeaconUsageLocal(
            @RequestParam(required = false, defaultValue = "xpipe") String system,
            @RequestParam(required = false, defaultValue = "false") boolean includeClusters) {
        return monitorManager.getSentinelBeaconUsage(system, includeClusters);
    }

    @GetMapping("/dr/usage")
    public RestResponse<Map<String, RegionBeaconUsage>> getDRBeaconUsage(
            @RequestParam(required = false, defaultValue = "xpipe") String system,
            @RequestParam(required = false, defaultValue = "false") boolean includeClusters) {
        return RestResponse.success(monitorManager.getDRBeaconUsage(system, includeClusters));
    }

    @GetMapping("/sentinel/cluster/{clusterName}")
    public Map<String, RestResponse<List<SentinelClusterBeaconRouteItem>>> getSentinelClusterBeaconRoute(
            @PathVariable String clusterName) {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            return new HashMap<>();
        }

        if (consoleServiceManager == null) {
            return groupLocalByDc(monitorManager.getSentinelClusterRoutes(clusterTbl.getClusterName()));
        }
        return consoleServiceManager.getAllConsoleSentinelClusterBeaconRoute(clusterTbl.getClusterName());
    }

    @GetMapping("/sentinel/cluster/{clusterName}/local")
    public List<SentinelClusterBeaconRouteItem> getSentinelClusterBeaconRouteLocal(@PathVariable String clusterName) {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            return new ArrayList<>();
        }
        return monitorManager.getSentinelClusterRoutes(clusterTbl.getClusterName());
    }

    @GetMapping("/dr/cluster/{clusterName}")
    public RestResponse<Map<String, DRClusterBeaconRouteItem>> getDRClusterBeaconRoute(@PathVariable String clusterName) {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            return RestResponse.fail("cluster not found: " + clusterName);
        }
        return RestResponse.success(monitorManager.getDRClusterRoutes(clusterTbl.getClusterName()));
    }

    private Map<String, RestResponse<List<SentinelClusterBeaconRouteItem>>> groupLocalByDc(List<SentinelClusterBeaconRouteItem> localData) {
        Map<String, List<SentinelClusterBeaconRouteItem>> grouped = new LinkedHashMap<>();
        for (SentinelClusterBeaconRouteItem item : localData) {
            grouped.computeIfAbsent(item.getDcName(), k -> new ArrayList<>()).add(item);
        }
        Map<String, RestResponse<List<SentinelClusterBeaconRouteItem>>> result = new LinkedHashMap<>();
        grouped.forEach((dc, data) -> result.put(dc, RestResponse.success(data)));
        return result;
    }

    private BeaconRouteType parseRouteType(String routeType) {
        if (routeType == null || routeType.isEmpty()) {
            return BeaconRouteType.SENTINEL;
        }
        return BeaconRouteType.valueOf(routeType.toUpperCase());
    }
}
