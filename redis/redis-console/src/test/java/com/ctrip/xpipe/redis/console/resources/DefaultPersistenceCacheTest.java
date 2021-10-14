package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.google.common.collect.Lists;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;

import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2021/3/21
 */
public class DefaultPersistenceCacheTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private PersistenceCache persistenceCache;

    @Autowired
    private ConfigService configService;

    @Mock
    CheckerConfig config;
    
    CheckerConfig oldConfig;
    
    @Before
    public void beforeDefaultPersistenceCacheTest() throws SQLException, ComponentLookupException, IOException {
        DefaultPersistenceCache cache = ((DefaultPersistenceCache)persistenceCache);
        oldConfig = cache.getConfig();
        cache.setConfig(config);
        when(config.getConfigCacheTimeoutMilli()).thenReturn(1L);
    }
    
    @After
    public void afterDefaultPersistenceCacheTest() {
        DefaultPersistenceCache cache = ((DefaultPersistenceCache)persistenceCache);
        cache.setConfig(oldConfig);
    }
    
    @Test
    public void testIsClusterOnMigration() {
        Assert.assertFalse(persistenceCache.isClusterOnMigration("cluster1"));
        Assert.assertTrue(persistenceCache.isClusterOnMigration("cluster2"));
    }

    @Test
    public void testAlertSystemOn() throws Exception {
        Assert.assertTrue(persistenceCache.isAlertSystemOn());
        configService.stopAlertSystem(new ConfigModel(), 1);
        Assert.assertFalse(persistenceCache.isAlertSystemOn());
        configService.startAlertSystem(new ConfigModel());
        waitConditionUntilTimeOut(() -> persistenceCache.isAlertSystemOn(), 10, 1);
    }

    @Test
    public void testSentinelAutoProcess() throws Exception {
        Assert.assertTrue(persistenceCache.isSentinelAutoProcess());
        configService.stopSentinelAutoProcess(new ConfigModel(), 1);
        Assert.assertFalse(persistenceCache.isSentinelAutoProcess());
        configService.startSentinelAutoProcess(new ConfigModel());
        waitConditionUntilTimeOut(() -> persistenceCache.isSentinelAutoProcess(), 10, 1);
    }

    @Test
    public void testGetClusterCreateTime() {
        Date date = persistenceCache.getClusterCreateTime("cluster1");
        Assert.assertNotNull(date);
        Assert.assertTrue(DateTimeUtils.getHoursBeforeDate(new Date(), 1).before(date));
        Assert.assertTrue(new Date().after(date));
    }

    @Test
    public void testSentinelAutoProcessCache() throws DalException, InterruptedException {
        when(config.getConfigCacheTimeoutMilli()).thenReturn(100L);
        Assert.assertTrue(persistenceCache.isSentinelAutoProcess());
        configService.stopSentinelAutoProcess(new ConfigModel(), 1);
        Assert.assertTrue(persistenceCache.isSentinelAutoProcess());
        Thread.sleep(100L);
        Assert.assertFalse(persistenceCache.isSentinelAutoProcess());
        configService.startSentinelAutoProcess(new ConfigModel());
        Assert.assertFalse(persistenceCache.isSentinelAutoProcess());
        Thread.sleep(100L);
        Assert.assertTrue(persistenceCache.isSentinelAutoProcess());
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/persistence-test.sql");
    }

}
