package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.TcpPortCheckCommand;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.InfoActionContext;
import com.ctrip.xpipe.redis.console.console.ConsoleService;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.ShardCheckerHealthCheckModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardAllMetaModel;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath.PATH_GET_DC_ISOLATED;
import static com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath.PATH_GET_INNER_DC_ISOLATED;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
public class DefaultConsoleService extends AbstractService implements ConsoleService{

    private String host;

    private int port;

    private String address;

    private final String healthStatusUrl;

    private final String crossRegionHealthStatusUrl;

    private final String allHealthStatusUrl;

    private final String allCrossRegionHealthStatusUrl;

    private final String allCrossRegionClusterHealthStatusUrl;

    private final String pingStatusUrl;

    private final String innerShardDelayStatusUrl;

    private final String innerDelayStatusUrl;

    private final String instancesDelayStatusUrl;

    private final String delayStatusUrl;

    private final String allDelayStatusUrl;

    private final String unhealthyInstanceUrl;

    private final String allUnhealthyInstanceUrl;

    private final String innerCrossMasterDelayUrl;

    private final String crossMasterDelayUrl;

    private final String allLocalRedisInfosUrl;

    private final String shardAllCheckerGroupHealthCheckUrl;

    private final String shardAllMetaUrl;

    private final String innerDcIsolatedUrl;

    private final String dcIsolatedUrl;

    private static final ParameterizedTypeReference<Map<HostPort, Long>> hostDelayTypeDef =
            new ParameterizedTypeReference<Map<HostPort, Long>>(){};

    private static final ParameterizedTypeReference<Map<String, Pair<HostPort, Long>>> crossMasterDelayDef =
            new ParameterizedTypeReference<Map<String, Pair<HostPort, Long>>>(){};

    public DefaultConsoleService(String host, int port) {
        if (host.startsWith("http://")) {
            this.host = host.split("http://")[1];
        } else {
            this.host = host;
        }
        this.port = port;
        this.address = "http://" + this.host + ":" + this.port;
        healthStatusUrl = String.format("%s/api/health/{ip}/{port}", this.address);
        crossRegionHealthStatusUrl = String.format("%s/api/cross/region/health/{ip}/{port}", this.address);
        allHealthStatusUrl = String.format("%s/api/health/check/status/all", this.address);
        allCrossRegionHealthStatusUrl = String.format("%s/api/health/cross/region/check/status/all", this.address);
        allCrossRegionClusterHealthStatusUrl = String.format("%s/api/health/check/instance/cluster/{clusterId}", this.address);
        pingStatusUrl = String.format("%s/api/redis/ping/{ip}/{port}", this.address);
        innerShardDelayStatusUrl = String.format("%s/api/shard/inner/delay/{shardId}", this.address);
        innerDelayStatusUrl = String.format("%s/api/redis/inner/delay/{ip}/{port}", this.address);
        instancesDelayStatusUrl = String.format("%s/api/redises/inner/delay", this.address);
        delayStatusUrl = String.format("%s/api/redis/delay/{ip}/{port}", this.address);
        allDelayStatusUrl = String.format("%s/api/redis/inner/delay/all", this.address);
        unhealthyInstanceUrl = String.format("%s/api/redis/inner/unhealthy", this.address);
        allUnhealthyInstanceUrl = String.format("%s/api/redis/inner/unhealthy/all", this.address);
        crossMasterDelayUrl = String.format("%s/api/cross-master/delay/{dcId}/{cluster}/{shard}", this.address);
        innerCrossMasterDelayUrl = String.format("%s/api/cross-master/inner/delay/{cluster}/{shard}", this.address);
        allLocalRedisInfosUrl = String.format("%s/api/redis/info/local", this.address);
        shardAllCheckerGroupHealthCheckUrl = String.format("%s/api/shard/checker/group/health/check/{dcId}/{clusterId}/{shardId}", this.address);
        shardAllMetaUrl = String.format("%s/api/shard/meta/{dcId}/{clusterId}/{shardId}", this.address);
        innerDcIsolatedUrl = String.format("%s%s", this.address, PATH_GET_INNER_DC_ISOLATED);
        dcIsolatedUrl = String.format("%s%s", this.address, PATH_GET_DC_ISOLATED);
    }

    @Override
    public HEALTH_STATE getInstanceStatus(String ip, int port) {
        return restTemplate.getForObject(healthStatusUrl, HEALTH_STATE.class, ip, port);
    }

    @Override
    public HEALTH_STATE getCrossRegionInstanceStatus(String ip, int port) {
        return restTemplate.getForObject(crossRegionHealthStatusUrl, HEALTH_STATE.class, ip, port);
    }

    @Override
    public Map<HostPort, HealthStatusDesc> getAllInstanceHealthStatus() {
        return restTemplate.getForObject(allHealthStatusUrl, AllInstanceHealthStatus.class);
    }

    @Override
    public Map<HostPort, HealthStatusDesc> getAllInstanceCrossRegionHealthStatus() {
        return restTemplate.getForObject(allCrossRegionHealthStatusUrl, AllInstanceHealthStatus.class);
    }

    @Override
    public Map<HostPort, HealthStatusDesc> getAllClusterInstanceHealthStatus(Set<HostPort> hostPorts) {
        return restTemplate.postForObject(allCrossRegionClusterHealthStatusUrl, hostPorts, AllInstanceHealthStatus.class);
    }

    @Override
    public List<ShardCheckerHealthCheckModel> getShardAllCheckerGroupHealthCheck(String dcId, String clusterId, String shardId) {
        return restTemplate.getForObject(shardAllCheckerGroupHealthCheckUrl, ShardCheckerHealthCheckModels.class, dcId, clusterId, shardId);
    }

    @Override
    public ShardAllMetaModel getShardAllMeta(String dcId, String clusterId, String shardId) {
        return restTemplate.getForObject(shardAllMetaUrl, ShardAllMetaModel.class, dcId, clusterId, shardId);
    }


    @Override
    public Boolean getInstancePingStatus(String ip, int port) {
        return restTemplate.getForObject(pingStatusUrl, Boolean.class, ip, port);
    }

    @Override
    public Long getInstanceDelayStatus(String ip, int port) {
        return restTemplate.getForObject(innerDelayStatusUrl, Long.class, ip, port);
    }

    @Override
    public Map<HostPort, Long> getInstancesDelayStatus(List<HostPort> hostPorts) {
        return restTemplate.postForObject(instancesDelayStatusUrl, hostPorts, InstancesDelayStatusModels.class);
    }

    @Override
    public Long getShardDelay(long shardId) {
        return restTemplate.getForObject(innerShardDelayStatusUrl, Long.class, shardId);
    }

    @Override
    public Long getInstanceDelayStatusFromParallelService(String ip, int port) {
        return restTemplate.getForObject(delayStatusUrl, Long.class, ip, port);
    }

    @Override
    public Map<HostPort, Long> getAllInstanceDelayStatus() {
        ResponseEntity<Map<HostPort, Long> > response = restTemplate.exchange(allDelayStatusUrl, HttpMethod.GET,
                null, hostDelayTypeDef);
        return response.getBody();
    }

    @Override
    public UnhealthyInfoModel getActiveClusterUnhealthyInstance() {
        return restTemplate.getForObject(unhealthyInstanceUrl, UnhealthyInfoModel.class);
    }

    @Override
    public UnhealthyInfoModel getAllUnhealthyInstance() {
        return restTemplate.getForObject(allUnhealthyInstanceUrl, UnhealthyInfoModel.class);
    }

    @Override
    public Map<String, Pair<HostPort, Long>> getCrossMasterDelay(String clusterId, String shardId) {
        ResponseEntity<Map<String, Pair<HostPort, Long>>> response = restTemplate.exchange(innerCrossMasterDelayUrl, HttpMethod.GET,
                null, crossMasterDelayDef, clusterId, shardId);
        return response.getBody();
    }

    @Override
    public Map<String, Pair<HostPort, Long>> getCrossMasterDelayFromParallelService(String sourceDcId, String clusterId, String shardId) {
        ResponseEntity<Map<String, Pair<HostPort, Long>>> response = restTemplate.exchange(crossMasterDelayUrl, HttpMethod.GET,
                null, crossMasterDelayDef, sourceDcId, clusterId, shardId);
        return response.getBody();
    }

    @Override
    public Map<HostPort, ActionContextRetMessage<Map<String, String>>> getAllLocalRedisInfos() {
        try {
            return restTemplate.getForObject(allLocalRedisInfosUrl, InfoActionContext.ResultMap.class);
        } catch (Throwable t) {
            return Collections.emptyMap();
        }
    }

    @Override
    public Boolean getInnerDcIsolated() {
        return restTemplate.getForObject(innerDcIsolatedUrl, Boolean.class);
    }

    @Override
    public Boolean getDcIsolated() {
        return restTemplate.getForObject(dcIsolatedUrl, Boolean.class);
    }

    @Override
    public CommandFuture<Boolean> connect(int connectTimeoutMilli) {
        return new TcpPortCheckCommand(host, port, connectTimeoutMilli).execute();
    }

    @Override
    public String toString() {
        return String.format("%s", address);
    }
}
