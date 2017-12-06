package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Dec 04, 2017
 */
public class ConfigControllerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    ConfigController controller;

    @Test
    public void testChangeConfig1() throws Exception {
        ConfigModel model = new ConfigModel();
        model.setKey(DefaultConsoleDbConfig.KEY_ALERT_SYSTEM_ON);
        model.setVal(String.valueOf(false));
        RetMessage ret = controller.changeConfig(model);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, ret.getState());
    }

    @Test
    public void testChangeConfig2() throws Exception {
        ConfigModel model = new ConfigModel();
        model.setKey("Key Unknown");
        model.setVal(String.valueOf(false));
        RetMessage ret = controller.changeConfig(model);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertEquals("Unknown config key: Key Unknown", ret.getMessage());
    }

}