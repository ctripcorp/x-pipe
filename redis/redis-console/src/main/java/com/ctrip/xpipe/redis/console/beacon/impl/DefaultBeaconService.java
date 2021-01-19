package com.ctrip.xpipe.redis.console.beacon.impl;

import com.ctrip.xpipe.redis.console.beacon.BeaconService;
import com.ctrip.xpipe.redis.console.beacon.data.BeaconClusterMeta;
import com.ctrip.xpipe.redis.console.beacon.data.BeaconResp;
import com.ctrip.xpipe.redis.console.beacon.exception.BeaconServiceException;
import com.ctrip.xpipe.redis.console.beacon.data.BeaconGroupMeta;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public class DefaultBeaconService extends AbstractService implements BeaconService {

    protected static final String PATH_GET_CLUSTERS = "/api/v1/monitor/xpipe/clusters";
    protected static final String PATH_CLUSTER = "/api/v1/monitor/xpipe/cluster/{cluster}";

    private String getAllClustersPath;
    private String clusterPath;

    private String host;

    private static final ParameterizedTypeReference<BeaconResp<Set<String>>> clustersRespTypeDef =
            new ParameterizedTypeReference<BeaconResp<Set<String>>>(){};

    public DefaultBeaconService(String host) {
        this.host = host;
        getAllClustersPath = host + PATH_GET_CLUSTERS;
        clusterPath = host + PATH_CLUSTER;
    }

    @Override
    public String getHost() {
        return this.host;
    }

    @Override
    public Set<String> fetchAllClusters() {
        ResponseEntity<BeaconResp<Set<String>>> responseEntity = restTemplate.exchange(getAllClustersPath, HttpMethod.GET, null, clustersRespTypeDef);
        BeaconResp<Set<String>> beaconResp = responseEntity.getBody();
        if (!beaconResp.isSuccess()) {
            logger.info("[fetchAllClusters] fail, {}", beaconResp.getMsg());
            throw new BeaconServiceException(getAllClustersPath, beaconResp.getCode(), beaconResp.getMsg());
        }

        return beaconResp.getData();
    }

    @Override
    public void registerCluster(String clusterName, Set<BeaconGroupMeta> groups) {
        HttpEntity<Object> entity = new HttpEntity<>(new BeaconClusterMeta(groups));
        ResponseEntity<BeaconResp> respResponseEntity = restTemplate.exchange(clusterPath, HttpMethod.POST, entity, BeaconResp.class, clusterName);
        BeaconResp beaconResp = respResponseEntity.getBody();
        if (!beaconResp.isSuccess()) {
            logger.info("[registerCluster] fail, {}", beaconResp.getMsg());
            throw new BeaconServiceException(clusterPath, beaconResp.getCode(), beaconResp.getMsg());
        }
    }

    @Override
    public void unregisterCluster(String clusterName) {
        ResponseEntity<BeaconResp> respResponseEntity = restTemplate.exchange(clusterPath, HttpMethod.DELETE, null, BeaconResp.class, clusterName);
        BeaconResp beaconResp = respResponseEntity.getBody();
        if (!beaconResp.isSuccess()) {
            logger.info("[unregisterCluster] fail, {}", beaconResp.getMsg());
            throw new BeaconServiceException(clusterPath, beaconResp.getCode(), beaconResp.getMsg());
        }
    }

    @Override
    public String toString() {
        return "DefaultBeaconService{" +
                "host='" + host + '\'' +
                '}';
    }
}
