package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.console.ConsoleService;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
public class DefaultConsoleService extends AbstractService implements ConsoleService{

    private String address;

    private final String healthStatusUrl;

    private final String pingStatusUrl;

    private final String innerDelayStatusUrl;

    private final String delayStatusUrl;

    private final String allDelayStatusUrl;

    private final String unhealthyInstanceUrl;

    private final String allUnhealthyInstanceUrl;

    private final String innerCrossMasterDelayUrl;

    private final String crossMasterDelayUrl;

    private static final ParameterizedTypeReference<Map<HostPort, Long>> hostDelayTypeDef =
            new ParameterizedTypeReference<Map<HostPort, Long>>(){};

    private static final ParameterizedTypeReference<Map<String, Pair<HostPort, Long>>> crossMasterDelayDef =
            new ParameterizedTypeReference<Map<String, Pair<HostPort, Long>>>(){};

    public DefaultConsoleService(String address){

        this.address = address;
        if(!this.address.startsWith("http://")){
            this.address = "http://" + this.address;
        }
        healthStatusUrl = String.format("%s/api/health/{ip}/{port}", this.address);
        pingStatusUrl = String.format("%s/api/redis/ping/{ip}/{port}", this.address);
        innerDelayStatusUrl = String.format("%s/api/redis/inner/delay/{ip}/{port}", this.address);
        delayStatusUrl = String.format("%s/api/redis/delay/{ip}/{port}", this.address);
        allDelayStatusUrl = String.format("%s/api/redis/inner/delay/all", this.address);
        unhealthyInstanceUrl = String.format("%s/api/redis/inner/unhealthy", this.address);
        allUnhealthyInstanceUrl = String.format("%s/api/redis/inner/unhealthy/all", this.address);
        crossMasterDelayUrl = String.format("%s/api/cross-master/delay/{dcId}/{cluster}/{shard}", this.address);
        innerCrossMasterDelayUrl = String.format("%s/api/cross-master/inner/delay/{cluster}/{shard}", this.address);
    }

    @Override
    public HEALTH_STATE getInstanceStatus(String ip, int port) {
        return restTemplate.getForObject(healthStatusUrl, HEALTH_STATE.class, ip, port);
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
    public String toString() {
        return String.format("%s", address);
    }
}
