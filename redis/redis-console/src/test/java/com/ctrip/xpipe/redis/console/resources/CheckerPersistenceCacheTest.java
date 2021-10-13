package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.config.DefaultConfig;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.resource.DefaultCheckerConsoleService;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.retry.RetryPolicyFactories;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.SetUtils;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE.INSTANCEUP;
import static org.mockito.Mockito.when;

public class CheckerPersistenceCacheTest extends AbstractCheckerTest {
    private MockWebServer webServer;
    @Mock
    CheckerConfig config;
    @Mock
    PersistenceCache persistence;

    @Test
    public void mockHttp() throws Exception {
        MockitoAnnotations.initMocks(this);
        webServer  = new MockWebServer();
        ObjectMapper objectMapper = new ObjectMapper();
        final CheckerConsoleService.AlertMessage[] acceptAlertMessage = new CheckerConsoleService.AlertMessage[1];
        final DefaultRedisInstanceInfo[]  instanceInfos = new DefaultRedisInstanceInfo[1];
        final Dispatcher dispatcher = new Dispatcher() {

            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                switch (request.getPath()) {
                    case ConsoleCheckerPath.PATH_GET_CLUSTER_ALERT_WHITE_LIST:
                        try {
                            return new MockResponse().setResponseCode(200).setBody(objectMapper.writeValueAsString(persistence.clusterAlertWhiteList())).setHeader("Content-Type", "application/json");
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        };
                        break;
                    case ConsoleCheckerPath.PATH_GET_LOAD_ALL_CLUSTER_CREATE_TIME:
                        try {
                            return new MockResponse().setResponseCode(200).setBody(objectMapper.writeValueAsString(persistence.loadAllClusterCreateTime())).setHeader("Content-Type", "application/json");
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        break;
                    case ConsoleCheckerPath.PATH_GET_IS_ALERT_SYSTEM_ON:
                        try {
                            return new MockResponse().setResponseCode(200).setBody(objectMapper.writeValueAsString(persistence.isAlertSystemOn())).setHeader("Content-Type", "application/json");
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        break;
                    case ConsoleCheckerPath.PATH_POST_RECORD_ALERT:
                        try {
                            String body = new String(request.getBody().readByteArray());
                            acceptAlertMessage[0] = objectMapper.readValue(body, CheckerConsoleService.AlertMessage.class);
                            return new MockResponse().setResponseCode(200);
                        } catch (IOException ioException) {
                            ioException.printStackTrace();
                        }
                        break;
                    case ConsoleCheckerPath.PATH_PERSISTENCE + "updateRedisRole/master":
                        try {
                            String body = new String(request.getBody().readByteArray());
                            instanceInfos[0] = objectMapper.readValue(body, DefaultRedisInstanceInfo.class);
                            return new MockResponse().setResponseCode(200);
                        } catch (IOException ioException) {
                            ioException.printStackTrace();
                        }
                        break;
                }
                return new MockResponse().setResponseCode(404);

            }
        };
        webServer.setDispatcher(dispatcher);
        int port = randomPort();
        webServer.start(port);
        when(config.getConsoleAddress()).thenReturn("http://127.0.0.1:" + port);
        when(config.getConfigCacheTimeoutMilli()).thenReturn(10L);
        Set<String> clusterAlertWhiteList = new LinkedHashSet<>();
        clusterAlertWhiteList.add("test");
        when(persistence.clusterAlertWhiteList()).thenReturn(clusterAlertWhiteList);
        Map<String,Date> map = Maps.newConcurrentMap();
        map.put("test",new Date(1L));
        when(persistence.loadAllClusterCreateTime()).thenReturn(map);

        CheckerConsoleService  service = new DefaultCheckerConsoleService();
        CheckerPersistenceCache checkerPersistenceCache = new CheckerPersistenceCache(config, service, scheduled);
        
       
        Assert.assertEquals(checkerPersistenceCache.clusterAlertWhiteList().size(), 1);
        Assert.assertEquals(checkerPersistenceCache.clusterAlertWhiteList().contains("test"), true);


        Date d = checkerPersistenceCache.getClusterCreateTime("test");
        Assert.assertEquals(d, new Date(1));

        when(persistence.isAlertSystemOn()).thenReturn(true);
        Assert.assertEquals(checkerPersistenceCache.isAlertSystemOn(), true);
        when(persistence.isAlertSystemOn()).thenReturn(false);
        Assert.assertEquals(checkerPersistenceCache.isAlertSystemOn(), true);
        Thread.sleep(10L);
        Assert.assertEquals(checkerPersistenceCache.isAlertSystemOn(), false);
        AlertEntity alertEntity = new AlertEntity(new HostPort("", 6379), "dc", "cluster", "shard", "message\nmessage1",
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
        Assert.assertEquals(acceptAlertMessage[0].getMessage().toString(), alertMessageEntity.toString());
        Assert.assertEquals(acceptAlertMessage[0].getEmailResponse().getProperties(), response.getProperties());

        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(1000);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(redisMeta.parent().parent().parent().getId(),
                redisMeta.parent().parent().getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                redisMeta.parent().getActiveDc(), ClusterType.BI_DIRECTION);
        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();
        instance.setInstanceInfo(info);
        instance.setEndpoint(new DefaultEndPoint(info.getHostPort().getHost(), info.getHostPort().getPort()));
        instance.setHealthCheckConfig(new DefaultHealthCheckConfig(buildCheckerConfig()));
        instance.setSession(new RedisSession(instance.getEndpoint(), scheduled, getXpipeNettyClientKeyedObjectPool()));
        checkerPersistenceCache.updateRedisRole(instance, Server.SERVER_ROLE.MASTER);
        Assert.assertEquals(instanceInfos[0].getDcId(), info.getDcId());
        Assert.assertEquals(instanceInfos[0].getHostPort(), info.getHostPort()); 
        Assert.assertEquals(instanceInfos[0].getActiveDc(), info.getActiveDc());
        Assert.assertEquals(instanceInfos[0].getShardId(), info.getShardId());
        Assert.assertEquals(instanceInfos[0].getClusterId(), info.getClusterId());
        Assert.assertEquals(instanceInfos[0].getClusterShardHostport(), info.getClusterShardHostport());
        Assert.assertEquals(instanceInfos[0].getClusterType(), info.getClusterType());
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
    
}

