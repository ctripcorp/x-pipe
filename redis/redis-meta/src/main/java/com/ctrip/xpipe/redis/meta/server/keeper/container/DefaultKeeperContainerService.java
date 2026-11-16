package com.ctrip.xpipe.redis.meta.server.keeper.container;

import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.HostNameVerification;
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
    private static final ParameterizedTypeReference<List<String>> stringListType = new
            ParameterizedTypeReference<List<String>>() {
            };
    private static final ParameterizedTypeReference<List<GcOrphanStoreResult>> gcOrphanStoreResultListType = new
            ParameterizedTypeReference<List<GcOrphanStoreResult>>() {
            };
    private KeeperContainerMeta keeperContainerMeta;
    private RestOperations restTemplate;
    private CurrentDcKeeperContainerAddressService addressService;

    public DefaultKeeperContainerService(KeeperContainerMeta keeperContainerMeta, RestOperations restTemplate) {
        this(keeperContainerMeta, restTemplate, null);
    }

    public DefaultKeeperContainerService(KeeperContainerMeta keeperContainerMeta, RestOperations restTemplate,
                                         CurrentDcKeeperContainerAddressService addressService) {
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
    public void releaseKeeper(KeeperTransMeta keeperTransMeta) {
        try {
            long clusterDbId = null == keeperTransMeta.getClusterDbId() ? 1:keeperTransMeta.getClusterDbId();
            long shardDbId = null == keeperTransMeta.getShardDbId() ? 1:keeperTransMeta.getShardDbId();
            String ip = resolveAddress();
            restTemplate.exchange("http://{ip}:{port}/keepers/clusters/{clusterId}/shards/{shardId}/release",
                    HttpMethod.PUT, new HttpEntity<Object>(keeperTransMeta, createHeaders()), Void.class,
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


    @Override
    public List<String> listStoreDirNames() {
        try {
            String ip = resolveAddress();
            ResponseEntity<List<String>> result = restTemplate.exchange("http://{ip}:{port}/keepers/gc/dirs",
                    HttpMethod.GET, new HttpEntity<>(createHeaders()), stringListType,
                    ip, keeperContainerMeta.getPort());
            return result.getBody();
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public GcOrphanStoreResult gcOrphanStore(Long replId) {
        try {
            String ip = resolveAddress();
            ResponseEntity<GcOrphanStoreResult> result = restTemplate.exchange("http://{ip}:{port}/keepers/gc/{replId}",
                    HttpMethod.PUT, new HttpEntity<>(createHeaders()), GcOrphanStoreResult.class,
                    ip, keeperContainerMeta.getPort(), replId);
            return result.getBody();
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public List<GcOrphanStoreResult> gcOrphanStores(List<Long> replIds) {
        try {
            String ip = resolveAddress();
            ResponseEntity<List<GcOrphanStoreResult>> result = restTemplate.exchange("http://{ip}:{port}/keepers/gc",
                    HttpMethod.PUT, new HttpEntity<>(replIds, createHeaders()), gcOrphanStoreResultListType,
                    ip, keeperContainerMeta.getPort());
            return result.getBody();
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }


    @Override
    public String resolveAddress() {
        String ip = keeperContainerMeta.getIp();
        String hostname = keeperContainerMeta.getHostname();

        if (StringUtil.isEmpty(hostname)) {
            return ip;
        }
        
        if (null == addressService) {
            throw new IllegalStateException("CurrentDcKeeperContainerAddressService not configured, cannot resolve hostname: " + hostname);
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
