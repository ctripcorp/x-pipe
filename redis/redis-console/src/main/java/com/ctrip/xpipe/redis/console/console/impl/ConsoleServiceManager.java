package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.ConsoleService;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.ShardCheckerHealthCheckModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardAllMetaModel;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Function;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
@Component
public class ConsoleServiceManager implements RemoteCheckerManager {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, ConsoleService> services = Maps.newConcurrentMap();

    private ConsoleService parallelService = null;

    private ConsoleConfig consoleConfig;

    private ConsoleLeaderElector leaderElector;

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

    @Override
    public Map<String, Boolean> getAllDcIsolatedCheckResult() {
        Map<String, Boolean> result = new HashMap<>();
        Map<String, ConsoleService> consoleServiceMap = loadAllConsoleServices();
        consoleServiceMap.forEach((key, value) -> result.put(key, value.getInnerDcIsolated()));
        return result;
    }

    @Override
    public Boolean getDcIsolatedCheckResult(String dcId) {
        ConsoleService consoleService = getServiceByDc(dcId);
        if (consoleService == null) {
            return null;
        }
        return consoleService.getDcIsolated();
    }

    @Override
    public CommandFuture<Boolean> connectDc(String dc, int connectTimeoutMilli) {
        ConsoleService consoleService = getServiceByDc(dc);
        return consoleService.connect(connectTimeoutMilli);
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

                    service = new DefaultConsoleService(consoleConfig.getConsoleDomains().get(optionalKey.get()), 80);
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

    private Map<String,ConsoleService> loadAllConsoleServices() {

        Map<String, ConsoleService> result = new HashMap<>();

        for(HostPort hostPort : getConsoleUrls()){
            String url = hostPort.toString();
            ConsoleService service = services.get(url);
            if (service == null) {
                service = new DefaultConsoleService(hostPort.getHost(), hostPort.getPort());
                services.put(url, service);
            }
            result.put(url, service);
        }
        return result;
    }

    private Set<HostPort>  getConsoleUrls(){

        Set<HostPort> consoleUrls = new HashSet<>();
        String port = System.getProperty("server.port", "8080");
        if(leaderElector != null) {
           List<String> servers = leaderElector.getAllServers();
           for(String server : servers){
               consoleUrls.add(new HostPort(server, Integer.parseInt(port)));
           }
        }

        return consoleUrls;
    }
}
