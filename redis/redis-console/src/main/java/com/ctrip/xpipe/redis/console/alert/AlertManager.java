package com.ctrip.xpipe.redis.console.alert;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 16, 2017
 */
@Component
public class AlertManager {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConsoleConfig consoleConfig;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private Set<String> alertClusterWhiteList;

    @PostConstruct
    public void postConstruct(){

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                alertClusterWhiteList = consoleConfig.getAlertWhileList();
            }
        }, 0, 30, TimeUnit.SECONDS);

    }


    public void forceAlert(String cluster, String shard, ALERT_TYPE type, String message){

        doAlert(cluster, shard, type, message, true);

    }


    public void alert(String cluster, String shard, ALERT_TYPE type, String message){

        doAlert(cluster, shard, type, message, false);
    }

    private void doAlert(String cluster, String shard, ALERT_TYPE type, String message, boolean force) {

        if(!force && !shouldAlert(cluster)){
            logger.warn("[alert][skip]{}, {}, {}, {}", cluster, shard, type, message);
            return;
        }

        logger.warn("[alert]{}, {}, {}, {}", cluster, shard, type, message);
        EventMonitor.DEFAULT.logAlertEvent(String.format("%s,%s,%s,%s", cluster, shard, type.simpleDesc(), message));

    }

    private boolean shouldAlert(String cluster) {
        return !alertClusterWhiteList.contains(cluster);
    }
}
