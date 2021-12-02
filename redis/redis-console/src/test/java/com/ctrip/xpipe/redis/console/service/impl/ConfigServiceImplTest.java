package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.console.AutoMigrationOffChecker;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2021/5/13
 */
public class ConfigServiceImplTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ConfigServiceImpl configService;

    private AutoMigrationOffChecker autoMigrationOffChecker;

    @Before
    public void setupConfigServiceImplTest() {
        autoMigrationOffChecker = Mockito.mock(AutoMigrationOffChecker.class);
        configService.setAutoMigrationOffChecker(autoMigrationOffChecker);
    }

    @Test
    public void testConfigAllowAutoMigration() throws Exception {
        Assert.assertTrue(configService.allowAutoMigration());
        configService.setAllowAutoMigration(false);

        Assert.assertFalse(configService.allowAutoMigration());
        Mockito.verify(autoMigrationOffChecker, Mockito.times(1)).startAlert();

        configService.setAllowAutoMigration(true);
        Assert.assertTrue(configService.allowAutoMigration());
    }

    @Test
    public void testResetCluster() throws Exception {
        ConfigModel configModel = new ConfigModel().setSubKey("cluster1");
        configService.stopClusterAlert(configModel, 30);
        configService.stopSentinelCheck(configModel, 30);
        Assert.assertFalse(configService.shouldSentinelCheck("cluster1"));
        Assert.assertFalse(configService.shouldAlert("cluster1"));

        configService.resetClusterWhitelist("cluster1");
        Assert.assertTrue(configService.shouldSentinelCheck("cluster1"));
        Assert.assertTrue(configService.shouldAlert("cluster1"));
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }

}
