package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.manager.NotificationManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

    @Autowired
    private NotificationManager notifier;

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


    public void forceAlert(String cluster, String shard, HostPort hostPort, ALERT_TYPE type, String message){

        doAlert(cluster, shard, hostPort, type, message, true);

    }


    public void alert(String cluster, String shard, HostPort hostPort, ALERT_TYPE type, String message){

        doAlert(cluster, shard, hostPort, type, message, false);
    }

    private void doAlert(String cluster, String shard, HostPort hostPort, ALERT_TYPE type, String message, boolean force) {

        if(!force && !shouldAlert(cluster)){
            logger.warn("[alert][skip]{}, {}, {}, {}", cluster, shard, type, message);
            return;
        }


        logger.warn("[alert]{}, {}, {}, {}", cluster, shard, type, message);
        EventMonitor.DEFAULT.logAlertEvent(String.format("%s,%s,%s,%s", cluster, shard, type.simpleDesc(), message));
        notifier.addAlert(cluster, shard, hostPort, type, message);
    }

    private boolean shouldAlert(String cluster) {
        return !alertClusterWhiteList.contains(cluster);
    }

}
