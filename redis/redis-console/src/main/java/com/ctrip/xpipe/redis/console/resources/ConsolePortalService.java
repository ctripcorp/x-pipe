package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.monitor.CatTransactionMonitor;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCreateInfo;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Service
public class ConsolePortalService extends AbstractService {

    private CatTransactionMonitor catTransactionMonitor = new CatTransactionMonitor();

    @Autowired
    private ConsoleConfig config;

    @Autowired
    private FoundationService foundationService;

    public List<AzGroupModel> getAllAzGroups() {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain()
                        + AbstractConsoleController.API_PREFIX + "/azGroup/all").build();
        ResponseEntity<List<AzGroupModel>> resp = exchange(comp.toUri(),
                HttpMethod.GET, null, new ParameterizedTypeReference<List<AzGroupModel>>(){}, "getAllAzGroups");
        return resp.getBody();
    }

    public List<ProxyModel> getAllProxy() {

        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() + "/api/proxies/all").build();

        ResponseEntity<List<ProxyModel>> resp = exchange(comp.toUri(),
                HttpMethod.GET, null, new ParameterizedTypeReference<List<ProxyModel>>(){}, "getAllProxy");
        return resp.getBody();
    }

    public List<ProxyModel> getMonitorActiveProxiesByDc() {
        String dc = foundationService.getDataCenter();
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() + "/api/proxies/monitor_active")
                .queryParam("dc", dc)
                .build();

        ResponseEntity<List<ProxyModel>> resp = exchange(comp.toUri(),
                HttpMethod.GET, null, new ParameterizedTypeReference<List<ProxyModel>>(){}, "getMonitorActiveProxiesByDc");

        return resp.getBody();
    }

    public XpipeMeta getXpipeAllMeta(long version) throws SAXException, IOException {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() + ConsoleCheckerPath.PATH_GET_ALL_META_LONG_PULL)
                .queryParam("format", "xml")
                .queryParam("version", version)
                .build();

        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.ACCEPT_ENCODING, "lz4");
        HttpEntity<String> entity = new HttpEntity<>(headers);

        ResponseEntity<String> raw = exchange(comp.toUri().toString(), HttpMethod.GET, entity, String.class, "getXpipeAllMeta");
        if (StringUtil.isEmpty(raw.getBody())) return null;
        return DefaultSaxParser.parse(raw.getBody());
    }

    public List<KeepercontainerTbl> getAllKeeperContainers() {

        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() +
                        AbstractConsoleController.API_PREFIX + "/keeper_container/all").build();

        ResponseEntity<List<KeepercontainerTbl>> resp = exchange(comp.toUri(), HttpMethod.GET, null,
                new ParameterizedTypeReference<List<KeepercontainerTbl>>(){}, "getAllKeeperContainers");
        return resp.getBody();
    }

    public boolean setConfig(ConfigModel model, Date util) {
        MultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
        params.add("config", model);
        if (util != null) {
            params.add("util", util);
        }
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(params);
        ResponseEntity<RetMessage> resp = exchange(config.getConsoleNoDbDomain() +
                AbstractConsoleController.API_PREFIX  + "/config/set", HttpMethod.POST, requestEntity, RetMessage.class, "setConfig");
        RetMessage retMessage = resp.getBody();
        return retMessage.getState() == 0;
    }

    public ConfigTbl getConfig(String key, String subId) {

        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() +
                AbstractConsoleController.API_PREFIX  + "/config")
                .queryParam("key", key)
                .queryParam("subId", subId).
                build();

        ResponseEntity<ConfigTbl> resp = exchange(comp.toUri().toString(),
                HttpMethod.GET, null, ConfigTbl.class, "getConfig");
        return resp.getBody();
    }

    public List<ConfigTbl> getAllConfigs(String key) {

        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() +
                AbstractConsoleController.API_PREFIX  + "/config/getAll/{key}").buildAndExpand(key);

        ResponseEntity<List<ConfigTbl>> resp = exchange(comp.toUri(),
                HttpMethod.GET, null, new ParameterizedTypeReference<List<ConfigTbl>>(){}, "getAllConfigs");
        return resp.getBody();
    }

    public List<ConfigTbl> findAllByKeyAndValueAndUntilAfter(String key, String value, Date until) {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() +
                        AbstractConsoleController.API_PREFIX  + "/config/findAll").build();
        MultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
        params.add("key", key);
        params.add("value", value);
        params.add("until", until.getTime());
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(params);
        ResponseEntity<List<ConfigTbl>> resp = exchange(comp.toUri(), HttpMethod.POST, requestEntity,
                new ParameterizedTypeReference<List<ConfigTbl>>(){}, "findAllByKeyAndValueAndUntilAfter");
        return resp.getBody();
    }

    public List<OrganizationTbl> getAllOrganizations() {

        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() +
                AbstractConsoleController.API_PREFIX  + "/organizations/all").build();

        ResponseEntity<List<OrganizationTbl>> resp = exchange(comp.toUri(), HttpMethod.GET, null,
                new ParameterizedTypeReference<List<OrganizationTbl>>(){}, "getAllOrganizations");
        return resp.getBody();
    }

    public List<DcTbl> getAllDcs() {

        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() +
                AbstractConsoleController.API_PREFIX  + "/dc_tbls").build();

        ResponseEntity<List<DcTbl>> resp = restTemplate.exchange(comp.toUri(), HttpMethod.GET, null,
                new ParameterizedTypeReference<List<DcTbl>>(){});
        return resp.getBody();
    }

    public List<DcTbl> findClusterRelatedDc(String clusterId) {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() +
                AbstractConsoleController.API_PREFIX  + "/clusters/" + clusterId + "/dcs").build();
        ResponseEntity<List<DcTbl>> resp = exchange(comp.toUri(), HttpMethod.GET, null,
                new ParameterizedTypeReference<List<DcTbl>>(){}, "findClusterRelatedDc");
        return resp.getBody();
    }

    public List<ClusterTbl> findAllClusters() {

        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() +
                AbstractConsoleController.API_PREFIX + "/clusters/all").build();

        ResponseEntity<List<ClusterTbl>> resp = exchange(comp.toUri(),
                HttpMethod.GET, null,
                new ParameterizedTypeReference<List<ClusterTbl>>(){}, "findAllClusters");
        return resp.getBody();
    }

    public List<ProxyTunnelInfo> findAllTunnelInfo() {

        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() +
                ConsoleCheckerPath.PATH_GET_PROXY_CHAINS).build();

        ResponseEntity<List<ProxyTunnelInfo>> resp = exchange(comp.toUri(),
                HttpMethod.GET, null,
                new ParameterizedTypeReference<List<ProxyTunnelInfo>>(){}, "findAllTunnelInfo");
        return resp.getBody();
    }

    public List<RedisTbl> findAllByDcClusterShard(long dcClusterShardId) {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() +
                AbstractConsoleController.API_PREFIX + "/redis/{dcClusterShardId}").buildAndExpand(dcClusterShardId);
        ResponseEntity<List<RedisTbl>> resp = exchange(comp.toUri(), HttpMethod.GET, null,
                new ParameterizedTypeReference<List<RedisTbl>>(){}, "findAllByDcClusterShard");
        return resp.getBody();
    }

    public void updateBatchKeeperActive(List<RedisTbl> redises) {
        HttpEntity<List<RedisTbl>> requestEntity = new HttpEntity<>(redises);
        ResponseEntity<RetMessage> resp = exchange(config.getConsoleNoDbDomain() +
                AbstractConsoleController.API_PREFIX + "/redis/updateBatchKeeperActive",
                HttpMethod.POST, requestEntity, RetMessage.class, "updateBatchKeeperActive"
        );
        RetMessage ret = resp.getBody();
        if(ret.getState() != RetMessage.SUCCESS_STATE) {
            throw new RuntimeException("update batch keeper active failed, state:" + ret.getState());
        }
    }

    public void insertRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> addrs) {
        RedisCreateInfo redisCreateInfo = new RedisCreateInfo();
        redisCreateInfo.setDcId(dcId);
        redisCreateInfo.setClusterId(clusterId);
        redisCreateInfo.setShardName(shardId);
        List<String> redisAddrs = new ArrayList<>();
        for (Pair<String, Integer> addr : addrs) {
            redisAddrs.add(addr.getKey() + ":" + addr.getValue());
        }
        redisCreateInfo.setRedises(redisAddrs.stream().collect(Collectors.joining(",")));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<RedisCreateInfo> requestEntity = new HttpEntity<>(redisCreateInfo, headers);
        ResponseEntity<RetMessage> resp = exchange(config.getConsoleNoDbDomain() +
                AbstractConsoleController.API_PREFIX  + "/redis/insert", HttpMethod.POST,
                requestEntity, RetMessage.class, "insertRedises");
        RetMessage retMessage = resp.getBody();
        if(retMessage.getState() != RetMessage.SUCCESS_STATE) {
            throw new RuntimeException(retMessage.getMessage());
        }
    }

    public void bindDc(DcClusterTbl dcClusterTbl) {
        HttpEntity<DcClusterTbl> requestEntity = new HttpEntity<>(dcClusterTbl);

        ResponseEntity<RetMessage> resp = exchange(config.getConsoleNoDbDomain() +
                        AbstractConsoleController.API_PREFIX + "/dc/bind",
                HttpMethod.POST, requestEntity, RetMessage.class, "bindDc"
        );
        RetMessage ret = resp.getBody();
        if(ret.getState() != RetMessage.SUCCESS_STATE) {
            throw new RuntimeException("bind dc fail:" + ret.getState());
        }
    }

    public List<RouteModel> getActiveRoutes() {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(config.getConsoleNoDbDomain() +
                AbstractConsoleController.API_PREFIX + "/routes/active").build();
        ResponseEntity<List<RouteModel>> resp = exchange(comp.toUri(),
                HttpMethod.GET, null, new ParameterizedTypeReference<List<RouteModel>>(){}, "getActiveRoutes"
        );
        return resp.getBody();
    }

    public void updateKeeperStatus(String dcId, String clusterId, String shardId, KeeperMeta newActiveKeeper) {
        try {
            catTransactionMonitor.logTransaction("ConsoleForwardAPI", "updateKeeperStatus",  new Callable<String>() {
                @Override
                public String call() {
                    restTemplate.put( config.getConsoleNoDbDomain() + "/api/dc/{dcId}/cluster/{clusterId}/shard/{shardId}/keepers/adjustment",
                            newActiveKeeper, dcId, clusterId, shardId);
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("[updateKeeperStatus]", e);
        }

    }

    <T> ResponseEntity<T> exchange(String url, HttpMethod var2, HttpEntity<?> httpEntity, Class<T> type, String name) {
        try {
            return catTransactionMonitor.logTransaction("ConsoleForwardAPI", name, new Callable<ResponseEntity<T>>() {
                @Override
                public ResponseEntity<T> call() {
                    return restTemplate.exchange(url, var2, httpEntity, type);
                }
            });
        } catch (Exception e) {
            logger.error("[exchange]", e);
            return null;
        }
    }

    <T> ResponseEntity<T> exchange(URI uri, HttpMethod var2, HttpEntity<?> httpEntity, ParameterizedTypeReference<T> type, String name) {
        try {
            return catTransactionMonitor.logTransaction("ConsoleForwardAPI", name, new Callable<ResponseEntity<T>>() {
                @Override
                public ResponseEntity<T> call() {
                    return restTemplate.exchange(uri, var2, httpEntity, type);
                }
            });
        } catch (Exception e) {
            logger.error("[exchange]", e);
            return null;
        }
    }

    public void setConsoleConfig(ConsoleConfig config) {
        this.config = config;
    }

}
