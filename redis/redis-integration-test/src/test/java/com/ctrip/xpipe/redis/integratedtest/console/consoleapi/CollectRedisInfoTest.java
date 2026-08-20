package com.ctrip.xpipe.redis.integratedtest.console.consoleapi;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.InfoActionContext;
import com.ctrip.xpipe.redis.core.foundation.IdcUtil;
import com.ctrip.xpipe.redis.integratedtest.console.AbstractXPipeClusterTest;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 03, 2021 4:31 PM
 */
public class CollectRedisInfoTest extends AbstractXPipeClusterTest {

    private Map<String, String> metaServers;

    private Map<String, String> consoles;

    private String zkJQ;

    private String zkOY;

    @Before
    public void setUpRedisInfoTest() {
        metaServers = new HashMap<>();

        consoles = new HashMap<>();
        consoles.put("jq", "http://127.0.0.1:8080");
        consoles.put("oy", "http://127.0.0.1:8081");

        zkJQ = "127.0.0.1:" + IdcUtil.JQ_ZK_PORT;
        zkOY = "127.0.0.1:" + IdcUtil.OY_ZK_PORT;
    }

    @After
    public void afterCollectRedisInfoTest() throws IOException {
        cleanupAllSubProcesses();
        killAllRedisServers();
        cleanupConf();
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/xpipe-dr.sql");
    }

    @Test
    public void testFullFeaturedConsole() throws Exception {

        startZk(IdcUtil.JQ_ZK_PORT);
        startZk(IdcUtil.OY_ZK_PORT);

        setUpTestDataSource();

        startRedis(6379);
        startRedis(7379);

        startConsole(8080, "jq", zkJQ, Collections.singletonList("127.0.0.1:8080"), consoles, metaServers);
        startConsole(8081, "oy", zkOY, Collections.singletonList("127.0.0.1:8081"), consoles, metaServers);

        waitConditionUntilTimeOut(this::isAllProcessAlive);

        waitForServerResp("http://127.0.0.1:8080/api/health/redis/info/127.0.0.1/6379", InfoActionContext.Result.class, 60000,
                (health)-> "master".equals(((InfoActionContext.Result) health).getPayload().get("role")));

        waitForServerResp("http://127.0.0.1:8080/api/health/redis/info/all", InfoActionContext.ResultMap.class, 60000,
                (healthAll)-> "master".equals(((InfoActionContext.ResultMap) healthAll).get(HostPort.fromString("127.0.0.1:6379")).getPayload().get("role")));

        waitForServerResp("http://127.0.0.1:8081/api/redis/info/global", InfoActionContext.ResultMap.class, 60000,
                (all)-> "master".equals(((InfoActionContext.ResultMap) all).get(HostPort.fromString("127.0.0.1:6379")).getPayload().get("role")));

        // section 过滤:?section=replication 只返回 Replication 段字段(role 等),
        // 不含其它段(如 Server 段的 redis_version)
        waitForServerResp("http://127.0.0.1:8081/api/redis/info/global?section=replication",
                InfoActionContext.ResultMap.class, 60000, (all) -> {
                    ActionContextRetMessage<Map<String, String>> r = ((InfoActionContext.ResultMap) all).get(HostPort.fromString("127.0.0.1:6379"));
                    Map<String, String> payload = r.getPayload();
                    return r != null && "master".equals(payload.get("role"))
                            && !payload.containsKey("redis_version");
                });

        // lz4 端到端:跨 DC /local 发 Accept-Encoding: lz4,服务端压缩,
        // 客户端 LZ4DecompressionInterceptor 自动解压并反序列化为 ResultMap(section=replication 字段过滤同样生效)
        RestOperations lz4RestTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();
        HttpHeaders lz4Headers = new HttpHeaders();
        lz4Headers.set(HttpHeaders.ACCEPT_ENCODING, "lz4");
        HttpEntity<?> lz4Entity = new HttpEntity<>(lz4Headers);
        ResponseEntity<InfoActionContext.ResultMap> localResp = lz4RestTemplate.exchange(
                "http://127.0.0.1:8080/api/redis/info/local?section=replication", HttpMethod.GET, lz4Entity,
                InfoActionContext.ResultMap.class);
        InfoActionContext.ResultMap localBody = localResp.getBody();
        Assert.assertNotNull(localBody);
        Map<String, String> localPayload = localBody.get(HostPort.fromString("127.0.0.1:6379")).getPayload();
        Assert.assertEquals("master", localPayload.get("role"));
        Assert.assertFalse(localPayload.containsKey("redis_version"));

        // checker /all 不压缩:即便发 Accept-Encoding: lz4,响应也无 Content-Encoding: lz4(checker 端点保持 Map)
        ResponseEntity<InfoActionContext.ResultMap> allResp = lz4RestTemplate.exchange(
                "http://127.0.0.1:8080/api/health/redis/info/all", HttpMethod.GET, lz4Entity,
                InfoActionContext.ResultMap.class);
        List<String> contentEncoding = allResp.getHeaders().get(HttpHeaders.CONTENT_ENCODING);
        Assert.assertTrue(contentEncoding == null || contentEncoding.isEmpty());
        Assert.assertNotNull(allResp.getBody().get(HostPort.fromString("127.0.0.1:6379")));
    }

    @Test
    public void testCheckerAndConsole() throws Exception {

        startZk(IdcUtil.JQ_ZK_PORT);
        startZk(IdcUtil.OY_ZK_PORT);

        setUpTestDataSource();

        startRedis(6379);
        startRedis(7379);

        startStandaloneConsole(8080, "jq", zkJQ, Collections.singletonList("127.0.0.1:8080"), consoles, Collections.emptyMap());
        startChecker(18080, "jq", zkJQ, Collections.singletonList("127.0.0.1:8080"));

        startStandaloneConsole(8081, "oy", zkOY, Collections.singletonList("127.0.0.1:8081"), consoles, Collections.emptyMap());
        startChecker(18081, "oy", zkOY, Collections.singletonList("127.0.0.1:8081"));

        waitConditionUntilTimeOut(this::isAllProcessAlive);

        waitForServerResp("http://127.0.0.1:18080/api/health/redis/info/127.0.0.1/6379", InfoActionContext.Result.class, 1200000,
                (health)-> "master".equals(((InfoActionContext.Result) health).getPayload().get("role")));

        waitForServerResp("http://127.0.0.1:18080/api/health/redis/info/all", InfoActionContext.ResultMap.class, 60000,
                (healthAll)-> "master".equals(((InfoActionContext.ResultMap) healthAll).get(HostPort.fromString("127.0.0.1:6379")).getPayload().get("role")));

        waitForServerResp("http://127.0.0.1:8081/api/redis/info/global", InfoActionContext.ResultMap.class, 60000,
                (all)-> "master".equals(((InfoActionContext.ResultMap) all).get(HostPort.fromString("127.0.0.1:6379")).getPayload().get("role")));
    }
}
