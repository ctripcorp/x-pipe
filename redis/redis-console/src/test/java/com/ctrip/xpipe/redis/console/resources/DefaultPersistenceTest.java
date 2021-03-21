package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Date;

/**
 * @author lishanglin
 * date 2021/3/21
 */
public class DefaultPersistenceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DefaultPersistence persistence;

    @Autowired
    private ConfigService configService;

    @Test
    public void testIsClusterOnMigration() {
        Assert.assertFalse(persistence.isClusterOnMigration("cluster1"));
        Assert.assertTrue(persistence.isClusterOnMigration("cluster2"));
    }

    @Test
    public void testAlertSystemOn() throws Exception {
        Assert.assertTrue(persistence.isAlertSystemOn());
        configService.stopAlertSystem(new ConfigModel(), 1);
        Assert.assertFalse(persistence.isAlertSystemOn());
        configService.startAlertSystem(new ConfigModel());
        Assert.assertTrue(persistence.isAlertSystemOn());
    }

    @Test
    public void testSentinelAutoProcess() throws Exception {
        Assert.assertTrue(persistence.isSentinelAutoProcess());
        configService.stopSentinelAutoProcess(new ConfigModel(), 1);
        Assert.assertFalse(persistence.isSentinelAutoProcess());
        configService.startSentinelAutoProcess(new ConfigModel());
        Assert.assertTrue(persistence.isSentinelAutoProcess());
    }

    @Test
    public void testGetClusterCreateTime() {
        Date date = persistence.getClusterCreateTime("cluster1");
        Assert.assertNotNull(date);
        Assert.assertTrue(DateTimeUtils.getHoursBeforeDate(new Date(), 1).before(date));
        Assert.assertTrue(new Date().after(date));
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/persistence-test.sql");
    }

}
