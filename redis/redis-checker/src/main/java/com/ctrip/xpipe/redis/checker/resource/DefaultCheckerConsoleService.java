package com.ctrip.xpipe.redis.checker.resource;

import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.model.*;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
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

    private static final ParameterizedTypeReference<Set<String>> stringSetRespTypeDef =
            new ParameterizedTypeReference<Set<String>>(){};
    private static final ParameterizedTypeReference<Map<String, OuterClientService.ClusterInfo>> clusterInfoMapTypeDef =
            new ParameterizedTypeReference<Map<String, OuterClientService.ClusterInfo>>(){};

    public XpipeMeta getXpipeMeta(String console, int clusterPartIndex) throws SAXException, IOException {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(console + ConsoleCheckerPath.PATH_GET_META)
                .queryParam("format", "xml")
                .buildAndExpand(clusterPartIndex);

        String raw = restTemplate.getForObject(comp.toString(), String.class);
        if (StringUtil.isEmpty(raw)) return null;
        return DefaultSaxParser.parse(raw);
    }

    private GroupCheckerLeaderElector clusterServer;

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

    @Override
    public void reportKeeperContainerInfo(String console, Map<HostPort, RedisMsg> redisMsgMap, int index) {
        try {
            restTemplate.postForEntity(console + ConsoleCheckerPath.PATH_POST_KEEPER_CONTAINER_INFO_RESULT,
                    redisMsgMap, RetMessage.class, index);

        } catch (Throwable th) {
            logger.error("report keeper used info fail : {}", index, th);
        }
    }

    @VisibleForTesting
    protected void setRestOperations(RestOperations restOperations) {
        this.restTemplate = restOperations;
    }

    @Override
    public boolean isClusterOnMigration(String console, String clusterId) {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(console + ConsoleCheckerPath.PATH_GET_IS_CLUSTER_ON_MIGRATION)
                .buildAndExpand(clusterId);
        Boolean result = restTemplate.getForObject(comp.toString() , Boolean.class);
        if (result == null) {
            throw new XpipeRuntimeException("result of isClusterOnMigration is null");
        }
        return result;
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
    public Set<String> migratingClusterList(String console) {
        ResponseEntity<Set<String>> response = restTemplate
                .exchange(console + ConsoleCheckerPath.PATH_GET_MIGRATING_CLUSTER_LIST, HttpMethod.GET, null, stringSetRespTypeDef);
        return response.getBody();
    }

    @Override
    public boolean isSentinelAutoProcess(String console) {
        Boolean result = restTemplate.getForObject(console + ConsoleCheckerPath.PATH_GET_IS_SENTINEL_AUTO_PROCESS, Boolean.class);
        if (result == null) {
            throw new XpipeRuntimeException("result of isSentinelAutoProcess is null");
        }
        return result;
    }

    @Override
    public boolean isAlertSystemOn(String console) {
        Boolean result = restTemplate.getForObject(console + ConsoleCheckerPath.PATH_GET_IS_ALERT_SYSTEM_ON, Boolean.class);
        if (result == null) {
            throw new XpipeRuntimeException("result of isAlertSystemOn is null");
        }
        return result;
    }

    @Override
    public boolean isKeeperBalanceInfoCollectOn(String console) {
        Boolean result = restTemplate.getForObject(console + ConsoleCheckerPath.PATH_GET_IS_KEEPER_BALANCE_INFO_COLLECT_ON, Boolean.class);
        if (result == null) {
            throw new XpipeRuntimeException("result of isKeeperBalanceInfoCollectOn is null");
        }
        return result;
    }

    @Override
    public Date getClusterCreateTime(String console, String clusterId) {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(console + ConsoleCheckerPath.PATH_GET_CLUSTER_CREATE_TIME)
                .buildAndExpand(clusterId);
        Date date = restTemplate.getForObject(comp.toString(), Date.class);
        if (date == null) {
            throw new XpipeRuntimeException("result of getClusterCreateTime is null");
        }
        return date;
    }

    @Override
    public Map<String, Date> loadAllClusterCreateTime(String console) {
        ResponseEntity<Map<String, Date>> times = restTemplate.exchange(
                console + ConsoleCheckerPath.PATH_GET_LOAD_ALL_CLUSTER_CREATE_TIME,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<Map<String, Date>>(){});
        return times.getBody();
    }

    @Override
    public Map<String, OuterClientService.ClusterInfo> loadAllDcOneWayClusterInfo(String console, String dc) {
        UriComponents comp = UriComponentsBuilder
                .fromHttpUrl(console + ConsoleCheckerPath.PATH_GET_ALL_CURRENT_DC_ACTIVE_DC_ONE_WAY_CLUSTERS)
                .queryParam("dc", dc).build();

        ResponseEntity<Map<String, OuterClientService.ClusterInfo>> times = restTemplate.exchange(
                comp.toString(), HttpMethod.GET, null, clusterInfoMapTypeDef);
        return times.getBody();
    }

    @Override
    public Map<String, OuterClientService.ClusterInfo> loadCurrentDcOneWayClusterInfo(String console, String dc) {
        UriComponents comp = UriComponentsBuilder
                .fromHttpUrl(console + ConsoleCheckerPath.PATH_GET_ALL_CURRENT_DC_ONE_WAY_CLUSTERS)
                .queryParam("dc", dc).build();

        ResponseEntity<Map<String, OuterClientService.ClusterInfo>> times = restTemplate.exchange(
                comp.toString(), HttpMethod.GET, null, clusterInfoMapTypeDef);
        return times.getBody();
    }

    @Override
    public void recordAlert(String console, String eventOperator, AlertMessageEntity message, EmailResponse response) {
        restTemplate.postForObject(console + ConsoleCheckerPath.PATH_POST_RECORD_ALERT, new AlertMessage(eventOperator,message, response), String.class);
    }

    @Override
    public void bindShardSentinel(String console, String dc, String cluster, String shard, SentinelMeta sentinelMeta) {
        restTemplate.postForObject(console + ConsoleCheckerPath.PATH_BIND_SHARD_SENTINEL+"/"+dc+"/"+cluster+"/"+shard, sentinelMeta, RetMessage.class);
    }
}
