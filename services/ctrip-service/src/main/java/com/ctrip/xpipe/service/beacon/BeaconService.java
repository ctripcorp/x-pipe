package com.ctrip.xpipe.service.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorShardMeta;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.service.beacon.data.BeaconResp;
import com.ctrip.xpipe.service.beacon.exception.BeaconServiceException;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.web.client.RestOperations;

import java.util.Objects;
import java.util.Set;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/1/26
 */
public class BeaconService implements MonitorService {

    private static final String METRIC_TYPE = "call.beacon";
    private static final String NO_CLUSTER = "-";

    RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplateWithRetry(1, 300);
    private MetricProxy metricProxy = MetricProxy.DEFAULT;

    protected static final String PATH_GET_CLUSTERS = "/api/v1/monitor/{system}/clusters";
    protected static final String PATH_CLUSTER = "/api/v1/monitor/{system}/cluster/{cluster}";
    protected static final String PATH_CLUSTER_EXTRA = "/api/v1/monitor/{system}/cluster/{cluster}/extra";
    protected static final String PATH_HASH_CLUSTER = "/api/v1/check/{system}/cluster/{cluster}/hash";
    protected static final String PATH_CHECK_ALL_CLUSTER = "/api/v1/check/{systemName}/clusters";

    private String getAllClustersPath;
    private String clusterPath;
    private String clusterExtraPath;
    private String hashPath;
    private String allClustersPath;

    private String name;
    private String host;
    private int weight;

    private final Logger logger = LoggerFactory.getLogger(BeaconService.class);

    private static final ParameterizedTypeReference<BeaconResp<Set<String>>> clustersRespTypeDef =
            new ParameterizedTypeReference<BeaconResp<Set<String>>>(){};

    public BeaconService(String name, String host, int weight) {
        this.name = name;
        this.weight = weight;
        updateHost(host);
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getHost() {
        return this.host;
    }

    @Override
    public int getWeight() {
        return this.weight;
    }

    @Override
    public void setWeight(int weight) {
        this.weight = weight;
    }

    @Override
    public void updateHost(String host) {
        this.host = host;
        getAllClustersPath = host + PATH_GET_CLUSTERS;
        clusterPath = host + PATH_CLUSTER;
        clusterExtraPath = host + PATH_CLUSTER_EXTRA;
        hashPath = host + PATH_HASH_CLUSTER;
        allClustersPath = host + PATH_CHECK_ALL_CLUSTER;
    }

    @Override
    public Set<String> fetchAllClusters(String system) {
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            ResponseEntity<BeaconResp<Set<String>>> responseEntity = restTemplate.exchange(getAllClustersPath, HttpMethod.GET, null, clustersRespTypeDef, system);
            BeaconResp<Set<String>> beaconResp = responseEntity.getBody();
            if (null == beaconResp) {
                throw new BeaconServiceException(getAllClustersPath, 0, "no body");
            } else if (!beaconResp.isSuccess()) {
                logger.info("[fetchAllClusters] fail, {}", beaconResp.getMsg());
                throw new BeaconServiceException(getAllClustersPath, beaconResp.getCode(), beaconResp.getMsg());
            }

            success = true;
            return beaconResp.getData();
        } finally {
            tryMetric("fetchAllClusters", HttpMethod.GET, system, NO_CLUSTER, success, startTime);
        }
    }

    @Override
    public void registerCluster(String system, String clusterName, Set<MonitorGroupMeta> groups, Map<String, String> extraData) {
        registerCluster(system, clusterName, groups, null, extraData, HttpMethod.POST);
    }

    @Override
    public void updateCluster(String system, String clusterName, Set<MonitorGroupMeta> groups, Map<String, String> extraData) {
        registerCluster(system, clusterName, groups, null, extraData, HttpMethod.PUT);
    }

    @Override
    public void registerCluster(String system, String clusterName, Set<MonitorGroupMeta> groups, Set<MonitorShardMeta> shards, Map<String, String> extraData) {
        registerCluster(system, clusterName, groups, shards, extraData, HttpMethod.POST);
    }

    @Override
    public void updateCluster(String system, String clusterName, Set<MonitorGroupMeta> groups, Set<MonitorShardMeta> shards, Map<String, String> extraData) {
        registerCluster(system, clusterName, groups, shards, extraData, HttpMethod.PUT);
    }

    private void registerCluster(String system, String clusterName, Set<MonitorGroupMeta> groups,
                                 Set<MonitorShardMeta> shards, Map<String, String> extraData, HttpMethod method) {
        logger.info("[registerCluster][{}][{}]", system, clusterName);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Object> entity = new HttpEntity<>(new MonitorClusterMeta(groups, shards, extraData), headers);
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            ResponseEntity<BeaconResp> respResponseEntity = restTemplate.exchange(clusterPath, method, entity, BeaconResp.class, system, clusterName);

            BeaconResp beaconResp = respResponseEntity.getBody();
            if (null == beaconResp) {
                throw new BeaconServiceException(clusterPath, 0, "no body");
            } else if (!beaconResp.isSuccess()) {
                logger.info("[registerCluster] fail, {}", beaconResp.getMsg());
                throw new BeaconServiceException(clusterPath, beaconResp.getCode(), beaconResp.getMsg());
            }
            success = true;
        } finally {
            if (HttpMethod.POST.equals(method)) {
                tryMetric("registerCluster", method, system, clusterName, success, startTime);
            }
        }
    }

    @Override
    public void unregisterCluster(String system, String clusterName) {
        if (system == null || clusterName == null) {
            return;
        }
        logger.info("[unregisterCluster][{}][{}]", system, clusterName);
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            ResponseEntity<BeaconResp> respResponseEntity = restTemplate.exchange(clusterPath, HttpMethod.DELETE,
                null, BeaconResp.class, system, clusterName);
            BeaconResp beaconResp = respResponseEntity.getBody();
            if (null == beaconResp) {
                throw new BeaconServiceException(clusterPath, 0, "no body");
            } else if (!beaconResp.isSuccess()) {
                logger.info("[unregisterCluster] fail, {}", beaconResp.getMsg());
                throw new BeaconServiceException(clusterPath, beaconResp.getCode(), beaconResp.getMsg());
            }
            success = true;
        } finally {
            tryMetric("unregisterCluster", HttpMethod.DELETE, system, clusterName, success, startTime);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BeaconService that = (BeaconService)o;

        if (weight != that.weight)
            return false;
        if (!Objects.equals(name, that.name))
            return false;
        return Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + weight;
        return result;
    }

    @Override
    public String toString() {
        return "BeaconService{" + "name='" + name + '\'' + ", host='" + host + '\'' + ", weight=" + weight + '}';
    }

    @Override
    public int getBeaconClusterHash(String system, String clusterName) {
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            ResponseEntity<BeaconResp<Integer>> respResponseEntity = restTemplate.exchange(hashPath, HttpMethod.GET, null, new ParameterizedTypeReference<BeaconResp<Integer>>(){}, system, clusterName);
            BeaconResp<Integer> beaconResp = respResponseEntity.getBody();
            if (null == beaconResp) {
                throw new BeaconServiceException(hashPath, 0, "no body");
            } else if (!beaconResp.isSuccess()) {
                logger.info("[getBeaconClusterHash] fail, {}", beaconResp.getMsg());
                throw new BeaconServiceException(hashPath, beaconResp.getCode(), beaconResp.getMsg());
            }
            success = true;
            return beaconResp.getData();
        } finally {
            tryMetric("getBeaconClusterHash", HttpMethod.GET, system, clusterName, success, startTime);
        }
    }

    private final ParameterizedTypeReference<BeaconResp<Map<String, String>>> extraDataTypeDef = new ParameterizedTypeReference<>(){};
    @Override
    public Map<String, String> getBeaconClusterExtra(String system, String clusterName) {
        ResponseEntity<BeaconResp<Map<String, String>>> responseEntity = restTemplate.exchange(clusterExtraPath, HttpMethod.GET,
                null, extraDataTypeDef, system, clusterName);
        BeaconResp<Map<String, String>> beaconResp = responseEntity.getBody();
        if (null == beaconResp) {
            throw new BeaconServiceException(clusterExtraPath, 0, "no body");
        } else if (!beaconResp.isSuccess()) {
            logger.info("[getBeaconClusterExtra] fail, {}", beaconResp.getMsg());
            throw new BeaconServiceException(clusterExtraPath, beaconResp.getCode(), beaconResp.getMsg());
        }
        return beaconResp.getData();
    }

    private final ParameterizedTypeReference<BeaconResp<Map<String, Set<String>>>> clusterWithDcTypeDef = new ParameterizedTypeReference<>() {};
    @Override
    public Map<String, Set<String>> getAllClusterWithDc(String system) {
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            ResponseEntity<BeaconResp<Map<String, Set<String>>>> responseEntity = restTemplate.exchange(allClustersPath,
                    HttpMethod.GET, null, clusterWithDcTypeDef, system);
            BeaconResp<Map<String, Set<String>>> beaconResp = responseEntity.getBody();
            if (null == beaconResp) {
                throw new BeaconServiceException(allClustersPath, 0, "no body");
            } else if (!beaconResp.isSuccess()) {
                logger.info("[getAllClusterWithDc] fail, {}", beaconResp.getMsg());
                throw new BeaconServiceException(allClustersPath, beaconResp.getCode(), beaconResp.getMsg());
            }

            success = true;
            return beaconResp.getData();
        } finally {
            tryMetric("getAllClusterWithDc", HttpMethod.GET, system, NO_CLUSTER, success, startTime);
        }
    }

    @VisibleForTesting
    protected void setMetricProxy(MetricProxy metricProxy) {
        this.metricProxy = metricProxy;
    }

    private void tryMetric(String operation, HttpMethod method, String system, String clusterName, boolean success,
                           long startTime) {
        try {
            long endTime = System.currentTimeMillis();
            MetricData metricData = new MetricData(METRIC_TYPE, null, clusterName, null);
            metricData.setValue(endTime - startTime);
            metricData.setTimestampMilli(startTime);
            metricData.addTag("operation", operation);
            metricData.addTag("method", method.name());
            metricData.addTag("success", String.valueOf(success));
            metricData.addTag("system", system);
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (Throwable th) {
            logger.warn("[tryMetric][{}][{}][{}] fail", operation, system, clusterName, th);
        }
    }
}

