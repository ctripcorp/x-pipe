package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.resource.DefaultCheckerConsoleService;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.google.common.collect.Lists;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import static com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath.PATH_PERSISTENCE;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CheckerPersistenceCacheTest extends AbstractCheckerTest {

    private MockWebServer webServer;

    private long cacheTimeoutMill = 10L;

    @Mock
    CheckerConfig config;

    @Before
    public void setupCheckerPersistenceCacheTest() throws Exception {
        this.webServer = new MockWebServer();
        this.webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());

        when(config.getConsoleAddress()).thenReturn("http://127.0.0.1:" + webServer.getPort());
        when(config.getConfigCacheTimeoutMilli()).thenReturn(cacheTimeoutMill);
    }

    @After
    public void afterCheckerPersistenceCacheTest() throws Exception {
        this.webServer.close();
    }

    @Test
    public void testClusterAlertWhitelist() throws Exception {
        CheckerPersistenceCache checkerPersistenceCache = new CheckerPersistenceCache(config, new DefaultCheckerConsoleService());
        webServer.enqueue(new MockResponse()
                .setBody(Codec.DEFAULT.encode(Collections.singleton("Cluster1")))
                .setHeader("Content-Type", "application/json"));

        Set<String> whitelist = checkerPersistenceCache.clusterAlertWhiteList();
        // turn whitelist into low case for cluster searching with case ignore
        Assert.assertEquals(Collections.singleton("cluster1"), whitelist);

        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals(ConsoleCheckerPath.PATH_GET_CLUSTER_ALERT_WHITE_LIST, req.getPath());
        Assert.assertEquals("GET", req.getMethod());
        Assert.assertEquals(1, webServer.getRequestCount());
    }

    @Test
    public void testRetError() throws Exception {
        CheckerPersistenceCache checkerPersistenceCache = new CheckerPersistenceCache(config, new DefaultCheckerConsoleService());
        webServer.enqueue(new MockResponse().setResponseCode(500));
        Assert.assertEquals(Collections.emptySet(), checkerPersistenceCache.clusterAlertWhiteList());
    }

    @Test
    public void testSentinelWhitelist() throws Exception {
        CheckerPersistenceCache checkerPersistenceCache = new CheckerPersistenceCache(config, new DefaultCheckerConsoleService());
        webServer.enqueue(new MockResponse()
                .setBody(Codec.DEFAULT.encode(Collections.singleton("Cluster1")))
                .setHeader("Content-Type", "application/json"));

        Set<String> whitelist = checkerPersistenceCache.sentinelCheckWhiteList();
        // turn whitelist into low case for cluster searching with case ignore
        Assert.assertEquals(Collections.singleton("cluster1"), whitelist);

        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals(ConsoleCheckerPath.PATH_GET_SENTINEL_CHECKER_WHITE_LIST, req.getPath());
        Assert.assertEquals("GET", req.getMethod());
        Assert.assertEquals(1, webServer.getRequestCount());
    }

    @Test
    public void testSentinelAutoProcess() throws Exception {
        CheckerPersistenceCache checkerPersistenceCache = new CheckerPersistenceCache(config, new DefaultCheckerConsoleService());
        webServer.enqueue(new MockResponse()
                .setBody("false")
                .setHeader("Content-Type", "application/json"));

        boolean autoProcess = checkerPersistenceCache.isSentinelAutoProcess();
        Assert.assertFalse(autoProcess);

        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals(ConsoleCheckerPath.PATH_GET_IS_SENTINEL_AUTO_PROCESS, req.getPath());
        Assert.assertEquals("GET", req.getMethod());
        Assert.assertEquals(1, webServer.getRequestCount());
    }

    @Test
    public void testAutoProcessExpired() {
        CheckerPersistenceCache checkerPersistenceCache = new CheckerPersistenceCache(config, new DefaultCheckerConsoleService());
        webServer.enqueue(new MockResponse()
                .setBody("false")
                .setHeader("Content-Type", "application/json"));
        webServer.enqueue(new MockResponse()
                .setBody("true")
                .setHeader("Content-Type", "application/json"));

        Assert.assertFalse(checkerPersistenceCache.isSentinelAutoProcess());
        Assert.assertFalse(checkerPersistenceCache.isSentinelAutoProcess());
        sleep((int) cacheTimeoutMill);
        Assert.assertTrue(checkerPersistenceCache.isSentinelAutoProcess());
    }

    @Test
    public void testAlertSystemOn() throws Exception {
        CheckerPersistenceCache checkerPersistenceCache = new CheckerPersistenceCache(config, new DefaultCheckerConsoleService());
        webServer.enqueue(new MockResponse()
                .setBody("false")
                .setHeader("Content-Type", "application/json"));

        boolean alertSystemOn = checkerPersistenceCache.isAlertSystemOn();
        Assert.assertFalse(alertSystemOn);

        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals(ConsoleCheckerPath.PATH_GET_IS_ALERT_SYSTEM_ON, req.getPath());
        Assert.assertEquals("GET", req.getMethod());
        Assert.assertEquals(1, webServer.getRequestCount());
    }

    @Test
    public void testAlertRecord() throws Exception {
        CheckerPersistenceCache checkerPersistenceCache = new CheckerPersistenceCache(config, new DefaultCheckerConsoleService());
        webServer.enqueue(new MockResponse());

        AlertEntity alertEntity = new AlertEntity(new HostPort("10.0.0.1", 6379),
                "dc", "cluster", "shard", "message\nmessage1",
                ALERT_TYPE.CLIENT_INCONSIS);
        AlertMessageEntity alertMessageEntity = new AlertMessageEntity("Test", "test", Lists.newArrayList("test-list"), alertEntity);
        Properties properties = new Properties();
        properties.setProperty("hello", "test");
        EmailResponse response = new EmailResponse() {
            @Override
            public Properties getProperties() {
                return properties;
            }
        };
        checkerPersistenceCache.recordAlert(FoundationService.DEFAULT.getLocalIp(), alertMessageEntity, response);

        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals(ConsoleCheckerPath.PATH_POST_RECORD_ALERT, req.getPath());
        Assert.assertEquals("POST", req.getMethod());
        Assert.assertEquals(1, webServer.getRequestCount());

        CheckerConsoleService.AlertMessage reqMsg = Codec.DEFAULT.decode(req.getBody().readByteArray(), CheckerConsoleService.AlertMessage.class);
        Assert.assertEquals(alertMessageEntity.toString(), reqMsg.getMessage().toString());
        Assert.assertEquals(response.getProperties(), reqMsg.getEmailResponse().getProperties());
    }

    @Test
    public void testUpdateRedisRole() throws Exception {
        CheckerPersistenceCache checkerPersistenceCache = new CheckerPersistenceCache(config, new DefaultCheckerConsoleService());
        webServer.enqueue(new MockResponse()
                .setBody(Codec.DEFAULT.encode(new RetMessage(RetMessage.SUCCESS_STATE)))
                .setHeader("Content-Type", "application/json"));

        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(1000);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(redisMeta.parent().parent().parent().getId(),
                redisMeta.parent().parent().getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                redisMeta.parent().getActiveDc(), ClusterType.BI_DIRECTION);
        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();
        instance.setInstanceInfo(info);
        instance.setEndpoint(new DefaultEndPoint(info.getHostPort().getHost(), info.getHostPort().getPort()));
        instance.setHealthCheckConfig(new DefaultHealthCheckConfig(buildCheckerConfig()));
        instance.setSession(new RedisSession(instance.getEndpoint(), scheduled, getXpipeNettyClientKeyedObjectPool(),buildCheckerConfig()));
        checkerPersistenceCache.updateRedisRole(instance, Server.SERVER_ROLE.MASTER);

        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals(PATH_PERSISTENCE + "updateRedisRole/master", req.getPath());
        Assert.assertEquals("PUT", req.getMethod());
        Assert.assertEquals(1, webServer.getRequestCount());
        DefaultRedisInstanceInfo reqInfo = Codec.DEFAULT.decode(req.getBody().readByteArray(), DefaultRedisInstanceInfo.class);

        Assert.assertEquals(info.getDcId(), reqInfo.getDcId());
        Assert.assertEquals(info.getHostPort(), reqInfo.getHostPort());
        Assert.assertEquals(info.getActiveDc(), reqInfo.getActiveDc());
        Assert.assertEquals(info.getShardId(), reqInfo.getShardId());
        Assert.assertEquals(info.getClusterId(), reqInfo.getClusterId());
        Assert.assertEquals(info.getClusterShardHostport(), reqInfo.getClusterShardHostport());
        Assert.assertEquals(info.getClusterType(), reqInfo.getClusterType());
    }

    @Test
    public void testDefaultRedisInstanceInfoJson() throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(1000);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(redisMeta.parent().parent().parent().getId(),
                redisMeta.parent().parent().getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                redisMeta.parent().getActiveDc(), ClusterType.BI_DIRECTION);
        String json = Codec.DEFAULT.encode(info);
        DefaultRedisInstanceInfo result = Codec.DEFAULT.decode(json, DefaultRedisInstanceInfo.class);
        Assert.assertEquals(result.toString(), info.toString());

    }

    @Test
    public void testAlertMessageEntityJson() {
        AlertEntity alertEntity = new AlertEntity(new HostPort("127.0.0.1", 6379), "dc", "cluster", "shard", "message\nmessage1",
                ALERT_TYPE.CLIENT_INCONSIS);
        
        AlertMessageEntity alertMessageEntity = new AlertMessageEntity("Test", "test", Lists.newArrayList("test-list"), alertEntity);
        String json = Codec.DEFAULT.encode(alertMessageEntity);
        AlertMessageEntity result = Codec.DEFAULT.decode(json, AlertMessageEntity.class);
        Assert.assertEquals(result.toString(), alertMessageEntity.toString());
    }

    @Test
    public void testIsClusterOnMigration() throws Exception {
        CheckerPersistenceCache checkerPersistenceCache = new CheckerPersistenceCache(config, new DefaultCheckerConsoleService());
        webServer.enqueue(new MockResponse()
                .setBody(Codec.DEFAULT.encode(Collections.singleton("Cluster1")))
                .setHeader("Content-Type", "application/json"));

        Set<String> migratingClusterList = checkerPersistenceCache.migratingClusterList();
        // turn cluster list into low case for cluster searching with case ignore
        Assert.assertEquals(Collections.singleton("Cluster1"), migratingClusterList);
        Assert.assertTrue(checkerPersistenceCache.isClusterOnMigration("Cluster1"));
        Assert.assertFalse(checkerPersistenceCache.isClusterOnMigration("Cluster2"));

        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals(ConsoleCheckerPath.PATH_GET_MIGRATING_CLUSTER_LIST, req.getPath());
        Assert.assertEquals("GET", req.getMethod());
        Assert.assertEquals(1, webServer.getRequestCount());
    }
    
}

