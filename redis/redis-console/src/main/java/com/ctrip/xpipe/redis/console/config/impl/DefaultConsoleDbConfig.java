package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
@Component
public class DefaultConsoleDbConfig extends AbstractConfigBean implements ConsoleDbConfig{

    public static final String KEY_SENTINEL_AUTO_PROCESS = "sentinel.auto.process";

    public static final String KEY_ALERT_SYSTEM_ON = "alert.system.on";

    public static final String KEY_IGNORE_MIGRATION_SYSTEM_AVAILABILITY = "ignore.migration.system.avail";

    @Autowired
    private DbConfig dbConfig;

    @Autowired
    private ConfigService configService;

    @PostConstruct
    public void postConstruct(){
        setConfig(dbConfig);
    }


    @Override
    public boolean isSentinelAutoProcess() {
        return configService.isSentinelAutoProcess();
    }

    @Override
    public boolean isAlertSystemOn() {
        return configService.isAlertSystemOn();
    }

    @Override
    public boolean ignoreMigrationSystemAvailability() {
        return configService.ignoreMigrationSystemAvailability();
    }

}
