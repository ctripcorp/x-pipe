package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.ConsoleService;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Function;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
@Component
public class ConsoleServiceManager {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, ConsoleService> services = Maps.newConcurrentMap();

    private ConsoleService parallelService = null;

    private ConsoleConfig consoleConfig;

    @Autowired
    public ConsoleServiceManager(ConsoleConfig consoleConfig) {
        this.consoleConfig = consoleConfig;
        String parallelConsoleDomain = consoleConfig.getParallelConsoleDomain();
        if (!StringUtil.isEmpty(parallelConsoleDomain)) {
            parallelService = new DefaultConsoleService(parallelConsoleDomain);
        }
    }

    public List<HEALTH_STATE> allHealthStatus(String ip, int port){

        Map<String, ConsoleService> consoleServiceMap = loadAllConsoleServices();
        List<HEALTH_STATE> result = new LinkedList<>();
        for(ConsoleService consoleService : consoleServiceMap.values()){

            try{
                HEALTH_STATE instanceStatus = consoleService.getInstanceStatus(ip, port);
                result.add(instanceStatus);
                logger.info("[allHealthStatus]{}, {}:{}, {}", consoleService, ip, port, instanceStatus);
            }catch (Exception e){
                logger.error("[allHealthStatus]" + consoleService + "," + ip + ":" + port, e);
            }
        }
        return result;
    }

    public long getDelay(String ip, int port, String activeIdc) {
        ConsoleService service = getServiceByDc(activeIdc);
        return service.getInstanceDelayStatus(ip, port);
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

        String allConsoles = consoleConfig.getAllConsoles();

        Set<String> consoleUrls = new HashSet<>();
        for(String sp : allConsoles.split("\\s*,\\s*")){

            if(StringUtil.isEmpty(sp)){
                continue;
            }
            consoleUrls.add(sp);
        }
        logger.debug("{}", consoleUrls);
        return consoleUrls;
    }
}
