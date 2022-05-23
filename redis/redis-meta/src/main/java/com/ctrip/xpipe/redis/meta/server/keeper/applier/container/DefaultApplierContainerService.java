package com.ctrip.xpipe.redis.meta.server.keeper.applier.container;

import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerErrorParser;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerService;
import com.ctrip.xpipe.redis.core.entity.ApplierContainerMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierTransMeta;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestOperations;

import java.util.List;

/**
 * @author ayq
 * <p>
 * 2022/4/2 12:31
 */
public class DefaultApplierContainerService implements ApplierContainerService {

    private static final ParameterizedTypeReference<List<ApplierInstanceMeta>> applierInstanceMetaListType = new
            ParameterizedTypeReference<List<ApplierInstanceMeta>>() {
            };
    private ApplierContainerMeta applierContainerMeta;
    private RestOperations restTemplate;

    public DefaultApplierContainerService(ApplierContainerMeta applierContainerMeta, RestOperations restTemplate) {
        this.applierContainerMeta = applierContainerMeta;
        this.restTemplate = restTemplate;
    }

    @Override
    public void addApplier(ApplierTransMeta applierTransMeta) {
        try {
            restTemplate.postForObject("http://{ip}:{port}/appliers", applierTransMeta, Void.class,
                    applierContainerMeta.getIp(), applierContainerMeta.getPort());
        } catch (HttpStatusCodeException ex) {
            throw ApplierContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void addOrStartApplier(ApplierTransMeta applierTransMeta) {
        try {
            restTemplate.postForObject("http://{ip}:{port}/appliers/clusters/{clusterId}/shards/{shardId}",
                    applierTransMeta, Void.class, applierContainerMeta.getIp(), applierContainerMeta.getPort(),
                    applierTransMeta.getClusterDbId(), applierTransMeta.getShardDbId());
        } catch (HttpStatusCodeException ex) {
            throw ApplierContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void removeApplier(ApplierTransMeta applierTransMeta) {
        try {
            restTemplate.exchange("http://{ip}:{port}/appliers/clusters/{clusterId}/shards/{shardId}",
                    HttpMethod.DELETE, new HttpEntity<Object>(applierTransMeta), Void.class,
                    applierContainerMeta.getIp(), applierContainerMeta.getPort(), applierTransMeta.getClusterDbId(),
                    applierTransMeta.getShardDbId());
        } catch (HttpStatusCodeException ex) {
            throw ApplierContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void startApplier(ApplierTransMeta applierTransMeta) {
        try {
            restTemplate.put("http://{ip}:{port}/appliers/clusters/{clusterId}/shards/{shardId}/start", applierTransMeta,
                    applierContainerMeta.getIp(), applierContainerMeta.getPort(), applierTransMeta.getClusterDbId(),
                    applierTransMeta.getShardDbId());
        } catch (HttpStatusCodeException ex) {
            throw ApplierContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public void stopApplier(ApplierTransMeta applierTransMeta) {
        try {
            restTemplate.put("http://{ip}:{port}/appliers/clusters/{clusterId}/shards/{shardId}/stop", applierTransMeta,
                    applierContainerMeta.getIp(), applierContainerMeta.getPort(), applierTransMeta.getClusterDbId(),
                    applierTransMeta.getShardDbId());
        } catch (HttpStatusCodeException ex) {
            throw ApplierContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }

    @Override
    public List<ApplierInstanceMeta> getAllAppliers() {
        try {
            ResponseEntity<List<ApplierInstanceMeta>> result = restTemplate.exchange("http://{ip}:{port}/appliers",
                    HttpMethod.GET, null, applierInstanceMetaListType,
                    applierContainerMeta.getIp(), applierContainerMeta.getPort());
            return result.getBody();
        } catch (HttpStatusCodeException ex) {
            throw ApplierContainerErrorParser.parseErrorFromHttpException(ex);
        }
    }
}
