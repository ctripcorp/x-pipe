package com.ctrip.xpipe.redis.integratedtest.console.config;

import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class SpringEnvConsoleConfig extends DefaultConsoleConfig implements ApplicationContextAware {

    @Override
    protected String getProperty(String key) {
        String property = null;
        if(applicationContext != null) {
            property = applicationContext.getEnvironment().getProperty(key);
        }
        if (property != null) {
            return property;
        }
        return super.getProperty(key);
    }

    @Override
    protected String getProperty(String key, String def) {
        String value = getProperty(key);
        if(value != null) {
            return value;
        }
        return def;
    }
    ApplicationContext applicationContext;
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}

