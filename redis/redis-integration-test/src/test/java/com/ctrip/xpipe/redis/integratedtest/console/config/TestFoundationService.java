package com.ctrip.xpipe.redis.integratedtest.console.config;

import com.ctrip.xpipe.api.foundation.FoundationService;
import org.junit.Assert;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import static com.ctrip.xpipe.foundation.DefaultFoundationService.*;

public class TestFoundationService implements FoundationService, ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public String getDataCenter() {
        Assert.assertNotNull(applicationContext);
        return applicationContext.getEnvironment().getProperty(DATA_CENTER_KEY);
    }

    @Override
    public String getAppId() {
        Assert.assertNotNull(applicationContext);
        return applicationContext.getEnvironment().getProperty(APP_ID_KEY);
    }

    @Override
    public String getLocalIp() {
        Assert.assertNotNull(applicationContext);
        return applicationContext.getEnvironment().getProperty(LOCAL_IP_KEY, "127.0.0.1");
    }

    @Override
    public String getGroupId() {
        Assert.assertNotNull(applicationContext);
        return applicationContext.getEnvironment().getProperty("groupId");
    }

    @Override
    public int getOrder() {
        Assert.assertNotNull(applicationContext);
        return HIGHEST_PRECEDENCE;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
