package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.ConsoleService;
import com.ctrip.xpipe.redis.console.health.action.HEALTH_STATE;
import com.ctrip.xpipe.utils.StringUtil;
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

    @Autowired
    private ConsoleConfig consoleConfig;

    @PostConstruct
    public void postConstruct(){

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
            result.put(url, new DefaultConsoleService(url));
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
