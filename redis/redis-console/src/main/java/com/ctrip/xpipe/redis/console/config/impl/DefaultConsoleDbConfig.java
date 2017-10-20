package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
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

    @Autowired
    private DbConfig dbConfig;

    @PostConstruct
    public void postConstruct(){
        setConfig(dbConfig);
    }


    @Override
    public boolean isSentinelAutoProcess() {

        return getBooleanProperty(KEY_SENTINEL_AUTO_PROCESS, true);
    }
}
