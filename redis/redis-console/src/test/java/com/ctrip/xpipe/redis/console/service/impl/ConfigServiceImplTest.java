package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.console.AutoMigrationOffChecker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

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

}
