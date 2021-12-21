package com.ctrip.xpipe.redis.meta.server.keeper.container;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorParser;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestOperations;

import java.util.List;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultKeeperContainerService implements KeeperContainerService {
    private static final ParameterizedTypeReference<List<KeeperInstanceMeta>> keeperInstanceMetaListType = new
            ParameterizedTypeReference<List<KeeperInstanceMeta>>() {
            };
    private KeeperContainerMeta keeperContainerMeta;
    private RestOperations restTemplate;

    public DefaultKeeperContainerService(KeeperContainerMeta keeperContainerMeta, RestOperations restTemplate) {
        this.keeperContainerMeta = keeperContainerMeta;
        this.restTemplate = restTemplate;
    }

    @Override
    public void addKeeper(KeeperTransMeta keeperTransMeta) {
        try {
            restTemplate.postForObject("http://{ip}:{port}/keepers", keeperTransMeta, Void.class,
                    keeperContainerMeta.getIp(), keeperContainerMeta.getPort());
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void removeKeeper(KeeperTransMeta keeperTransMeta) {
        try {
            restTemplate.exchange("http://{ip}:{port}/keepers/clusters/{clusterId}/shards/{shardId}",
                    HttpMethod.DELETE, new HttpEntity<Object>(keeperTransMeta), Void.class,
                    keeperContainerMeta.getIp(), keeperContainerMeta.getPort(), keeperTransMeta.getClusterDbId(),
                    keeperTransMeta.getShardDbId());
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void startKeeper(KeeperTransMeta keeperTransMeta) {
        try {
            restTemplate.put("http://{ip}:{port}/keepers/clusters/{clusterId}/shards/{shardId}/start", keeperTransMeta,
                    keeperContainerMeta.getIp(), keeperContainerMeta.getPort(), keeperTransMeta.getClusterDbId(),
                    keeperTransMeta.getShardDbId());
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void stopKeeper(KeeperTransMeta keeperTransMeta) {
        try {
            restTemplate.put("http://{ip}:{port}/keepers/clusters/{clusterId}/shards/{shardId}/stop", keeperTransMeta,
                    keeperContainerMeta.getIp(), keeperContainerMeta.getPort(), keeperTransMeta.getClusterDbId(),
                    keeperTransMeta.getShardDbId());
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public List<KeeperInstanceMeta> getAllKeepers() {
        try {
            ResponseEntity<List<KeeperInstanceMeta>> result = restTemplate.exchange("http://{ip}:{port}/keepers",
                    HttpMethod.GET, null, keeperInstanceMetaListType,
                    keeperContainerMeta.getIp(), keeperContainerMeta.getPort());
            return result.getBody();
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void addOrStartKeeper(KeeperTransMeta keeperTransMeta) {
        try {
            restTemplate.postForObject("http://{ip}:{port}/keepers/clusters/{clusterId}/shards/{shardId}",
                    keeperTransMeta, Void.class, keeperContainerMeta.getIp(), keeperContainerMeta.getPort(),
                    keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId());
        } catch (HttpStatusCodeException ex) {
            throw KeeperContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }
}
