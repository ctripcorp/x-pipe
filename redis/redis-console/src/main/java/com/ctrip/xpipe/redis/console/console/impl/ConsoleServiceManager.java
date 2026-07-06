package com.ctrip.xpipe.redis.console.console.impl;


import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.ConsoleService;
import com.ctrip.xpipe.redis.console.controller.api.dto.ApiResponse;
import com.ctrip.xpipe.redis.console.controller.api.dto.BeaconUsageItem;
import com.ctrip.xpipe.redis.console.controller.api.dto.ClusterBeaconRouteItem;
import com.ctrip.xpipe.redis.console.exception.NotEnoughResultsException;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.ShardCheckerHealthCheckModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardAllMetaModel;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

import static com.ctrip.xpipe.redis.console.resources.AbstractMetaCache.CURRENT_IDC;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
@Component
public class ConsoleServiceManager implements RemoteCheckerManager {

    private static final long BROADCAST_TIMEOUT_MILLI = 5000L;

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, ConsoleService> services = Maps.newConcurrentMap();

    private ConsoleService parallelService = null;

    private ConsoleConfig consoleConfig;

    private ConsoleLeaderElector leaderElector;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService globalExecutor;

    @Autowired
    public ConsoleServiceManager(ConsoleConfig consoleConfig, @Nullable ConsoleLeaderElector leaderElector) {
        this.consoleConfig = consoleConfig;
        this.leaderElector = leaderElector;
    }

    @Override
    public List<HEALTH_STATE> getHealthStates(String ip, int port){

        Map<String, ConsoleService> consoleServiceMap = loadAllConsoleServices();
        List<HEALTH_STATE> result = new LinkedList<>();
        for(ConsoleService consoleService : consoleServiceMap.values()){

            try{
                HEALTH_STATE instanceStatus = consoleService.getInstanceStatus(ip, port);
                result.add(instanceStatus);
                logger.info("[getHealthStates]{}, {}:{}, {}", consoleService, ip, port, instanceStatus);
            }catch (Exception e){
                logger.error("[getHealthStates]" + consoleService + "," + ip + ":" + port, e);
            }
        }
        return result;
    }

    @Override
    public List<Map<HostPort, HealthStatusDesc>> allInstanceHealthStatus() {
        Map<String, ConsoleService> consoleServiceMap = loadAllConsoleServices();
        List<Map<HostPort, HealthStatusDesc>> result = new LinkedList<>();

        for(ConsoleService consoleService : consoleServiceMap.values()){
            try{
                Map<HostPort, HealthStatusDesc> allInstanceHealthStatus = consoleService.getAllInstanceHealthStatus();
                result.add(allInstanceHealthStatus);
            }catch (Exception e){
                logger.error("[allInstanceHealthStatus] {}", consoleService, e);
            }
        }
        return result;
    }

    @Override
    public List<CheckerService> getAllCheckerServices() {
        return new ArrayList<>(loadAllConsoleServices().values());
    }


    public Map<String, Boolean> getAllDcIsolatedCheckResult() {
        Map<String, Boolean> result = new HashMap<>();
        Map<String, ConsoleService> consoleServiceMap = loadAllConsoleServices();
        consoleServiceMap.forEach((key, value) -> {
            try {
                Boolean innerIsolated = value.getInnerDcIsolated();
                if (innerIsolated != null)
                    result.put(key, innerIsolated);
            } catch (Throwable th) {
                logger.error("[getAllDcIsolatedCheckResult]{}", key, th);
            }
        });
        if (result.size() < consoleServiceMap.size()) {
            throw new NotEnoughResultsException("getAllDcIsolatedCheckResult");
        }
        return result;
    }


    public Boolean getDcIsolatedCheckResult(String dcId) {
        ConsoleService consoleService = getServiceByDc(dcId);
        if (consoleService == null) {
            return null;
        }
        return consoleService.getDcIsolated();
    }

    public List<String> dcsInCurrentRegion() {
        ConsoleService consoleService = getServiceByDc(CURRENT_IDC);
        return consoleService.dcsInCurrentRegion();
    }

    public List<ShardCheckerHealthCheckModel> getShardAllCheckerGroupHealthCheck(String activeDc, String dcId, String clusterId, String shardId) {
        ConsoleService service = getServiceByDc(activeDc);
        return service.getShardAllCheckerGroupHealthCheck(dcId, clusterId, shardId);
    }

    public ShardAllMetaModel getShardAllMeta(String dcId, String clusterId, String shardId) {
        ConsoleService service = getServiceByDc(dcId);
        return service.getShardAllMeta(dcId, clusterId, shardId);
    }

    public long getDelay(String ip, int port, String activeIdc) {
        ConsoleService service = getServiceByDc(activeIdc);
        return service.getInstanceDelayStatus(ip, port);
    }

    public Map<HostPort, Long> getDelay(List<HostPort> hostPorts, String activeIdc) {
        ConsoleService service = getServiceByDc(activeIdc);
        return service.getInstancesDelayStatus(hostPorts);
    }

    public long getShardDelay(long shardId, String activeIdc) {
        ConsoleService service = getServiceByDc(activeIdc);
        return service.getShardDelay(shardId);
    }

    public Map<String, Pair<HostPort, Long>> getCrossMasterDelay(String sourceIdc, String clusterId, String shardId) {
        ConsoleService service = getServiceByDc(sourceIdc);
        return service.getCrossMasterDelay(clusterId, shardId);
    }

    public Map<HostPort, Long> getAllDelay(String activeIdc) {
        ConsoleService service = getServiceByDc(activeIdc);
        return service.getAllInstanceDelayStatus();
    }

    public UnhealthyInfoModel getUnhealthyInstanceByIdc(String activeIdc) {
        ConsoleService service = getServiceByDc(activeIdc);
        return service.getActiveClusterUnhealthyInstance();
    }

    public long getDelayFromParallelService(String ip, int port) {
        if (null == parallelService) return -1L;
        return parallelService.getInstanceDelayStatusFromParallelService(ip, port);
    }

    public Map<String, Pair<HostPort, Long>> getCrossMasterDelayFromParallelService(String sourceDcId, String clusterId, String shardId) {
        if (null == parallelService) return Collections.emptyMap();
        return parallelService.getCrossMasterDelayFromParallelService(sourceDcId, clusterId, shardId);
    }

    public UnhealthyInfoModel getAllUnhealthyInstanceFromParallelService() {
        if (null == parallelService) return null;
        return parallelService.getAllUnhealthyInstance();
    }

    private ConsoleService getServiceByDc(String dcId) {
        String upperCaseDcId = dcId.toUpperCase();
        ConsoleService service = services.get(upperCaseDcId);
        if (service == null) {
            synchronized (this) {
                service = services.get(upperCaseDcId);
                if (service == null) {
                    Optional<String> optionalKey = consoleConfig.getConsoleDomains().keySet().stream().filter(dcId::equalsIgnoreCase).findFirst();
                    if (!optionalKey.isPresent()) {
                        throw new XpipeRuntimeException("unknown dc id " + dcId);
                    }

                    service = new DefaultConsoleService(consoleConfig.getConsoleDomains().get(optionalKey.get()));
                    services.put(upperCaseDcId, service);
                }
            }
        }

        return service;
    }

    public List<Boolean> allPingStatus(String host, int port) {
        Map<String, ConsoleService> consoleServiceMap = loadAllConsoleServices();
        List<Boolean> result = new LinkedList<>();
        for(ConsoleService consoleService : consoleServiceMap.values()){

            try{
                Boolean instancePingStatus = consoleService.getInstancePingStatus(host, port);
                result.add(instancePingStatus);
                logger.info("[allPingStatus]{}, {}:{}, {}", consoleService, host, port, instancePingStatus);
            }catch (Exception e){
                logger.error("[allPingStatus]" + consoleService + "," + host + ":" + port, e);
            }
        }
        return result;
    }

    public Map<HostPort, ActionContextRetMessage<Map<String, String>>> getLocalRedisInfosByDc(String dcId) {
        ConsoleService service = getServiceByDc(dcId);
        return service.getAllLocalRedisInfos();
    }

    public RetMessage preMigrateSentinelBeacon(String dcId, String clusterName) {
        return getServiceByDc(dcId).preMigrateSentinelBeacon(clusterName);
    }

    public RetMessage postMigrateSentinelBeacon(String dcId, String clusterName, Map<String, HostPort> shardMasters) {
        return getServiceByDc(dcId).postMigrateSentinelBeacon(clusterName, shardMasters);
    }

    private <T> Map<String, BroadcastResult<T>> broadcast(Set<String> dcIds, Function<String, T> perDcTask) {
        Map<String, BroadcastResult<T>> result = new LinkedHashMap<>();
        if (dcIds == null || dcIds.isEmpty()) return result;

        ParallelCommandChain chain = new ParallelCommandChain(globalExecutor);
        Map<String, CommandFuture<T>> futures = new LinkedHashMap<>();

        for (String dcId : dcIds) {
            AbstractCommand<T> cmd = new AbstractCommand<T>() {
                @Override
                protected void doExecute() throws Throwable {
                    future().setSuccess(perDcTask.apply(dcId));
                }
                @Override
                protected void doReset() {}
                @Override
                public String getName() { return "Broadcast-" + dcId; }
            };
            chain.add(cmd);
            futures.put(dcId, cmd.future());
        }

        boolean timedOut = false;
        try {
            timedOut = !chain.execute().await(BROADCAST_TIMEOUT_MILLI, TimeUnit.MILLISECONDS);
            if (timedOut) {
                logger.warn("[broadcast] timeout after {}ms, collecting partial results", BROADCAST_TIMEOUT_MILLI);
            }
        } catch (Throwable th) {
            logger.error("[broadcast] chain fail, collecting partial results", th);
        }
        futures.forEach((dcId, future) -> {
            if (future.isSuccess()) {
                result.put(dcId.toUpperCase(), BroadcastResult.success(future.getNow()));
            } else if (!future.isDone()) {
                result.put(dcId.toUpperCase(), BroadcastResult.fail("broadcast timeout after " + BROADCAST_TIMEOUT_MILLI + "ms"));
            } else {
                Throwable cause = future.cause();
                String msg = cause != null ? cause.getClass().getSimpleName() + ": " + cause.getMessage()
                                           : "broadcast failed";
                logger.warn("[broadcast][{}] failed: {}", dcId, msg, cause);
                result.put(dcId.toUpperCase(), BroadcastResult.fail(msg));
            }
        });

        return result;
    }

    private static final class BroadcastResult<T> {
        final T value;
        final String errorMsg;

        private BroadcastResult(T value, String errorMsg) {
            this.value = value;
            this.errorMsg = errorMsg;
        }

        static <T> BroadcastResult<T> success(T value) {
            return new BroadcastResult<>(value, null);
        }

        static <T> BroadcastResult<T> fail(String msg) {
            return new BroadcastResult<>(null, msg);
        }

        boolean isSuccess() {
            return errorMsg == null;
        }
    }

    public Map<String, ApiResponse<List<BeaconUsageItem>>> getAllConsoleBeaconUsage(
            String system, boolean includeClusters,
            Supplier<List<BeaconUsageItem>> localSupplier) {
        return aggregateAcrossConsoles(
                consoleConfig.getConsoleDomains().keySet(),
                localSupplier,
                dcId -> getServiceByDc(dcId).getBeaconUsage(system, includeClusters));
    }

    public Map<String, ApiResponse<List<ClusterBeaconRouteItem>>> getAllConsoleClusterBeaconRoute(
            String clusterName, Set<String> targetDcs,
            Supplier<List<ClusterBeaconRouteItem>> localSupplier) {
        return aggregateAcrossConsoles(
                targetDcs,
                localSupplier,
                dcId -> getServiceByDc(dcId).getClusterBeaconRoute(clusterName));
    }

    private <T> Map<String, ApiResponse<T>> aggregateAcrossConsoles(
            Set<String> targetDcs,
            Supplier<T> localSupplier,
            Function<String, T> remoteFetch) {
        String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();
        Map<String, ApiResponse<T>> result = new LinkedHashMap<>();

        try {
            result.put(currentDc, ApiResponse.success(localSupplier.get()));
        } catch (Exception e) {
            logger.error("[aggregateAcrossConsoles] local fail, dc={}", currentDc, e);
            result.put(currentDc, ApiResponse.fail("local failed"));
        }

        Set<String> remoteDcs = new LinkedHashSet<>();
        if (targetDcs != null) {
            for (String dc : targetDcs) {
                if (!dc.equalsIgnoreCase(currentDc)) {
                    remoteDcs.add(dc);
                }
            }
        }
        Map<String, BroadcastResult<T>> raw = broadcast(remoteDcs, remoteFetch);
        raw.forEach((dc, r) -> result.put(dc,
                r.isSuccess() ? ApiResponse.success(r.value) : ApiResponse.fail(r.errorMsg)));

        return result;
    }

    public <T> boolean quorumSatisfy(List<T> results, Function<T, Boolean> predicate){

        int count = 0;

        for(T t : results){
            if(predicate.apply(t)){
                    count++;
            }
        }
        return count >= quorum();
    }


    private int quorum(){

        return consoleConfig.getQuorum();
    }

    @VisibleForTesting
    Map<String,ConsoleService> loadAllConsoleServices() {

        Map<String, ConsoleService> result = new HashMap<>();

        for(String url : getConsoleUrls()){
            ConsoleService service = services.get(url);
            if (service == null) {
                service = new DefaultConsoleService(url);
                services.put(url, service);
            }
            result.put(url, service);
        }
        return result;
    }

    private Set<String>  getConsoleUrls(){

        Set<String> consoleUrls = new HashSet<>();
        String port = System.getProperty("server.port", "8080");
        if(leaderElector != null) {
           List<String> servers = leaderElector.getAllServers();
           for(String server : servers){
               consoleUrls.add(server + ":" + port);
           }
        }

        return consoleUrls;
    }

}
