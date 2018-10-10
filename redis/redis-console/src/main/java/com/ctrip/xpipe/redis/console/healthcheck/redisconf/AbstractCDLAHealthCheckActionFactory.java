package com.ctrip.xpipe.redis.console.healthcheck.redisconf;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 07, 2018
 */
public abstract class AbstractCDLAHealthCheckActionFactory implements CrossDcLeaderAwareHealthCheckActionFactory {

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    protected ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    protected ExecutorService executors;

    @Autowired
    protected AlertManager alertManager;

    @Autowired
    private AlertPolicyManager alertPolicyManager;

    @Autowired
    private ConsoleConfig consoleConfig;

    @PostConstruct
    public void registerAlertTypes() {
        for(ALERT_TYPE alertType : alertTypes()) {
            alertPolicyManager.markCheckInterval(alertType, ()->consoleConfig.getRedisConfCheckIntervalMilli());
        }
    }

    protected abstract List<ALERT_TYPE> alertTypes();

}
