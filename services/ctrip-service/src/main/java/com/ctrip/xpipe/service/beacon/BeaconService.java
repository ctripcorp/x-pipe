package com.ctrip.xpipe.service.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.service.beacon.data.BeaconResp;
import com.ctrip.xpipe.service.beacon.exception.BeaconServiceException;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.web.client.RestOperations;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/26
 */
public class BeaconService implements MonitorService {

    RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplateWithRetry(1, 300);

    protected static final String PATH_GET_CLUSTERS = "/api/v1/monitor/{system}/clusters";
    protected static final String PATH_CLUSTER = "/api/v1/monitor/{system}/cluster/{cluster}";

    private String getAllClustersPath;
    private String clusterPath;

    private String host;

    private Logger logger = LoggerFactory.getLogger(BeaconService.class);

    private static final ParameterizedTypeReference<BeaconResp<Set<String>>> clustersRespTypeDef =
            new ParameterizedTypeReference<BeaconResp<Set<String>>>(){};

    public BeaconService(String host) {
        this.host = host;
        getAllClustersPath = host + PATH_GET_CLUSTERS;
        clusterPath = host + PATH_CLUSTER;
    }

    @Override
    public String getHost() {
        return this.host;
    }

    @Override
    public Set<String> fetchAllClusters(String system) {
        ResponseEntity<BeaconResp<Set<String>>> responseEntity = restTemplate.exchange(getAllClustersPath, HttpMethod.GET, null, clustersRespTypeDef, system);
        BeaconResp<Set<String>> beaconResp = responseEntity.getBody();
        if (!beaconResp.isSuccess()) {
            logger.info("[fetchAllClusters] fail, {}", beaconResp.getMsg());
            throw new BeaconServiceException(getAllClustersPath, beaconResp.getCode(), beaconResp.getMsg());
        }

        return beaconResp.getData();
    }

    @Override
    public void registerCluster(String system, String clusterName, Set<MonitorGroupMeta> groups) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Object> entity = new HttpEntity<>(new MonitorClusterMeta(groups), headers);
        ResponseEntity<BeaconResp> respResponseEntity = restTemplate.exchange(clusterPath, HttpMethod.POST, entity, BeaconResp.class, system, clusterName);

        BeaconResp beaconResp = respResponseEntity.getBody();
        if (!beaconResp.isSuccess()) {
            logger.info("[registerCluster] fail, {}", beaconResp.getMsg());
            throw new BeaconServiceException(clusterPath, beaconResp.getCode(), beaconResp.getMsg());
        }
    }

    @Override
    public void unregisterCluster(String system, String clusterName) {
        ResponseEntity<BeaconResp> respResponseEntity = restTemplate.exchange(clusterPath, HttpMethod.DELETE, null, BeaconResp.class, system, clusterName);
        BeaconResp beaconResp = respResponseEntity.getBody();
        if (!beaconResp.isSuccess()) {
            logger.info("[unregisterCluster] fail, {}", beaconResp.getMsg());
            throw new BeaconServiceException(clusterPath, beaconResp.getCode(), beaconResp.getMsg());
        }
    }

    @Override
    public String toString() {
        return "BeaconService{" +
                "host='" + host + '\'' +
                '}';
    }
}

