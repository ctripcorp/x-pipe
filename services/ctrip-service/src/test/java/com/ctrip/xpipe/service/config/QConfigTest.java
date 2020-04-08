package com.ctrip.xpipe.service.config;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.service.AbstractServiceTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class QConfigTest extends AbstractServiceTest {

    @Test
    public void testQConfig(){

        Config config = Config.DEFAULT;
        Assert.assertTrue(config instanceof QConfig);



        String result = config.get("test");
        logger.info("test value:{}", result);
        config.addConfigChangeListener(new ConfigChangeListener() {

            @Override
            public void onChange(String key, String oldValue, String newValue) {
                logger.info("{}:{}->{}", key, oldValue, newValue);
            }
        });

    }

    @After
    public void afterQConfigTest() throws IOException {
        waitForAnyKeyToExit();
    }
}
