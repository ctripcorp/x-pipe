package com.ctrip.xpipe.redis.meta.server.keeper.container;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.HostNameVerification;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorParser;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestOperations;

import java.util.List;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultKeeperContainerService implements KeeperContainerService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerService.class);
    private static final ParameterizedTypeReference<List<KeeperInstanceMeta>> keeperInstanceMetaListType = new
            ParameterizedTypeReference<List<KeeperInstanceMeta>>() {
            };
    private KeeperContainerMeta keeperContainerMeta;
    private RestOperations restTemplate;
    private KeeperContainerAddressService addressService;

    public DefaultKeeperContainerService(KeeperContainerMeta keeperContainerMeta, RestOperations restTemplate) {
        this(keeperContainerMeta, restTemplate, null);
    }

    public DefaultKeeperContainerService(KeeperContainerMeta keeperContainerMeta, RestOperations restTemplate,
                                         KeeperContainerAddressService addressService) {
        this.keeperContainerMeta = keeperContainerMeta;
        this.restTemplate = restTemplate;
        this.addressService = addressService;
    }

    @Override
    public KeeperInstanceMeta infoPort(int queryPort) {
        try {
            String ip = resolveAddress();
            return restTemplate.getForObject("http://{ip}:{port}/keepers/port/{queryPort}", KeeperInstanceMeta.class,
                    ip, keeperContainerMeta.getPort(), queryPort);
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void addKeeper(KeeperTransMeta keeperTransMeta) {
        try {
            String ip = resolveAddress();
            restTemplate.postForObject("http://{ip}:{port}/keepers", 
                    new HttpEntity<>(keeperTransMeta, createHeaders()), Void.class, ip, keeperContainerMeta.getPort());
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void removeKeeper(KeeperTransMeta keeperTransMeta) {
        try {
            long clusterDbId = null == keeperTransMeta.getClusterDbId() ? 1:keeperTransMeta.getClusterDbId();
            long shardDbId = null == keeperTransMeta.getShardDbId() ? 1:keeperTransMeta.getShardDbId();
            String ip = resolveAddress();
            restTemplate.exchange("http://{ip}:{port}/keepers/clusters/{clusterId}/shards/{shardId}",
                    HttpMethod.DELETE, new HttpEntity<Object>(keeperTransMeta, createHeaders()), Void.class,
                    ip, keeperContainerMeta.getPort(), clusterDbId, shardDbId);
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void startKeeper(KeeperTransMeta keeperTransMeta) {
        try {
            String ip = resolveAddress();
            restTemplate.put("http://{ip}:{port}/keepers/clusters/{clusterId}/shards/{shardId}/start", 
                    new HttpEntity<>(keeperTransMeta, createHeaders()),
                    ip, keeperContainerMeta.getPort(), keeperTransMeta.getClusterDbId(),
                    keeperTransMeta.getShardDbId());
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void stopKeeper(KeeperTransMeta keeperTransMeta) {
        try {
            String ip = resolveAddress();
            restTemplate.put("http://{ip}:{port}/keepers/clusters/{clusterId}/shards/{shardId}/stop", 
                    new HttpEntity<>(keeperTransMeta, createHeaders()),
                    ip, keeperContainerMeta.getPort(), keeperTransMeta.getClusterDbId(),
                    keeperTransMeta.getShardDbId());
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public List<KeeperInstanceMeta> getAllKeepers() {
        try {
            String ip = resolveAddress();
            ResponseEntity<List<KeeperInstanceMeta>> result = restTemplate.exchange("http://{ip}:{port}/keepers",
                    HttpMethod.GET, new HttpEntity<>(createHeaders()), keeperInstanceMetaListType,
                    ip, keeperContainerMeta.getPort());
            return result.getBody();
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void addOrStartKeeper(KeeperTransMeta keeperTransMeta) {
        try {
            String ip = resolveAddress();
            restTemplate.postForObject("http://{ip}:{port}/keepers/clusters/{clusterId}/shards/{shardId}",
                    new HttpEntity<>(keeperTransMeta, createHeaders()), Void.class, 
                    ip, keeperContainerMeta.getPort(),
                    keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId());
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    /**
     * 解析容器地址: IP 有值优先用 IP；IP 无值时用 hostname 反查 IP
     */
    private String resolveAddress() {
        String ip = keeperContainerMeta.getIp();
        String hostname = keeperContainerMeta.getHostname();
        
        // 物理机形态: IP 有值直接用
        if (!StringUtil.isEmpty(ip) && !"0.0.0.0".equals(ip)) {
            return ip;
        }
        
        // 容器形态: IP 为空或 0.0.0.0，用 hostname 反查
        if (StringUtil.isEmpty(hostname)) {
            throw new IllegalStateException("keepercontainer has neither valid ip nor hostname: " + keeperContainerMeta);
        }
        
        if (null == addressService) {
            throw new IllegalStateException("KeeperContainerAddressService not configured, cannot resolve hostname: " + hostname);
        }
        
        String resolvedIp = addressService.getIpByHostname(hostname);
        if (null == resolvedIp) {
            throw new IllegalStateException("no alive keepercontainer for hostname: " + hostname);
        }
        
        logger.debug("[resolveAddress]{} -> {}", hostname, resolvedIp);
        return resolvedIp;
    }

    /**
     * 创建带 hostname 校验头的 HTTP headers
     */
    private HttpHeaders createHeaders() {
        HttpHeaders headers = new HttpHeaders();
        String hostname = keeperContainerMeta.getHostname();
        if (!StringUtil.isEmpty(hostname)) {
            headers.set(HostNameVerification.HEADER_TARGET_HOST_NAME, hostname);
        }
        return headers;
    }
}
