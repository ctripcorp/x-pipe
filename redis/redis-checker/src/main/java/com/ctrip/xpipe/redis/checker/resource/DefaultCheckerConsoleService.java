package com.ctrip.xpipe.redis.checker.resource;

import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import com.ctrip.xpipe.redis.checker.model.HealthCheckResult;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/16
 */
@Lazy
@Component
public class DefaultCheckerConsoleService extends AbstractService implements CheckerConsoleService {
    
    private static final ParameterizedTypeReference<List<ProxyTunnelInfo>> proxyTunnelInfosTypeDef = new ParameterizedTypeReference<List<ProxyTunnelInfo>>(){};

    public XpipeMeta getXpipeMeta(String console, int clusterPartIndex) throws SAXException, IOException {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(console + ConsoleCheckerPath.PATH_GET_META)
                .queryParam("format", "xml")
                .buildAndExpand(clusterPartIndex);

        String raw = restTemplate.getForObject(comp.toString(), String.class);
        if (StringUtil.isEmpty(raw)) return null;
        return DefaultSaxParser.parse(raw);
    }
    
    public XpipeMeta getXpipeAllMeta(String console) throws  SAXException, IOException {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(console + ConsoleCheckerPath.PATH_GET_ALL_META)
                .queryParam("format", "xml").build();

        String raw = restTemplate.getForObject(comp.toString(), String.class);
        if (StringUtil.isEmpty(raw)) return null;
        return DefaultSaxParser.parse(raw);
    }

    public List<ProxyTunnelInfo> getProxyTunnelInfos(String console) {
        ResponseEntity<List<ProxyTunnelInfo>> resp = restTemplate.exchange(console + ConsoleCheckerPath.PATH_GET_PROXY_CHAINS,
                HttpMethod.GET, null, proxyTunnelInfosTypeDef);
        return resp.getBody();
    }

    @Override
    public void ack(String console, CheckerStatus checkerStatus) {
        restTemplate.put(console + ConsoleCheckerPath.PATH_PUT_CHECKER_STATUS, checkerStatus);
    }

    @Override
    public void report(String console, HealthCheckResult result) {
        restTemplate.put(console + ConsoleCheckerPath.PATH_PUT_HEALTH_CHECK_RESULT, result);
    }

    @VisibleForTesting
    protected void setRestOperations(RestOperations restOperations) {
        this.restTemplate = restOperations;
    }

    @Override
    public boolean isClusterOnMigration(String console, String clusterId) {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(console + ConsoleCheckerPath.PATH_GET_IS_CLUSTER_ON_MIGRATION)
                .buildAndExpand(clusterId);
        return restTemplate.getForObject(comp.toString() , Boolean.class);
    }

    @Override
    public void updateRedisRole(String console, RedisHealthCheckInstance instance, Server.SERVER_ROLE role) {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(console + ConsoleCheckerPath.PATH_PUT_UPDATE_REDIS_ROLE)
                .buildAndExpand(role.toString());
        restTemplate.put(comp.toString() , instance.getCheckInfo());
    }

    @Override
    public Set<String> sentinelCheckWhiteList(String console) {
        return restTemplate.getForObject(console + ConsoleCheckerPath.PATH_GET_SENTINEL_CHECKER_WHITE_LIST, Set.class);
    }

    @Override
    public Set<String> clusterAlertWhiteList(String console) {
        return restTemplate.getForObject(console + ConsoleCheckerPath.PATH_GET_CLUSTER_ALERT_WHITE_LIST, Set.class);
    }

    @Override
    public boolean isSentinelAutoProcess(String console) {
        return restTemplate.getForObject(console + ConsoleCheckerPath.PATH_GET_IS_SENTINEL_AUTO_PROCESS, Boolean.class);
    }

    @Override
    public boolean isAlertSystemOn(String console) {
        return restTemplate.getForObject(console + ConsoleCheckerPath.PATH_GET_IS_ALERT_SYSTEM_ON, Boolean.class);
    }

    @Override
    public Date getClusterCreateTime(String console, String clusterId) {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(console + ConsoleCheckerPath.PATH_GET_CLUSTER_CREATE_TIME)
                .buildAndExpand(clusterId);
        Long time = restTemplate.getForObject(comp.toString(), Long.class);
        return new Date(time);
    }

    @Override
    public Map<String, Date> loadAllClusterCreateTime(String console) {
        Map<String, Object> times = restTemplate.getForObject(console + ConsoleCheckerPath.PATH_GET_LOAD_ALL_CLUSTER_CREATE_TIME, Map.class);
        Map<String, Date> dates = Maps.newConcurrentMap();
        times.entrySet().stream().forEach(entry -> {
            Object value = entry.getValue();
            if(value instanceof Long) {
                dates.put(entry.getKey(), new Date((long)value));
            } else if(value instanceof Integer) {
                dates.put(entry.getKey(), new Date((int)value));
            } else {
                throw new RuntimeException("type fail :" + value.getClass());
            }
        });
        return dates;
    }
    
    @Override
    public void recordAlert(String console, String eventOperator, AlertMessageEntity message, EmailResponse response) {
        restTemplate.postForObject(console + ConsoleCheckerPath.PATH_POST_RECORD_ALERT, new AlertMessage(eventOperator,message, response), String.class);
    }
}
