package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.ConsoleService;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
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

    @Autowired
    private ConsoleConfig consoleConfig;

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
        ConsoleService service = services.get(activeIdc);
        if (service == null) {
            synchronized (this) {
                service = services.get(activeIdc);
                if (service == null) {
                    service = new DefaultConsoleService(consoleConfig.getConsoleDomains().get(activeIdc.toUpperCase()));
                    services.put(activeIdc, service);
                }
            }
        }
        return service.getInstanceDelayStatus(ip, port);
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

    public void setConsoleConfig(ConsoleConfig consoleConfig) {
        this.consoleConfig = consoleConfig;
    }
}
