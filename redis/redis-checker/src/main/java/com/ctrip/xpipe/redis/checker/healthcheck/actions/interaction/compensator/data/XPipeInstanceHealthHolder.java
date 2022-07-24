package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;

import java.util.*;

/**
 * @author lishanglin
 * date 2022/7/21
 */
public class XPipeInstanceHealthHolder {

    private List<Map<HostPort, HealthStatusDesc>> healthCheckResult;

    public XPipeInstanceHealthHolder() {
        this(new ArrayList<>(4));
    }

    public XPipeInstanceHealthHolder(List<Map<HostPort, HealthStatusDesc>> healthCheckResult) {
        this.healthCheckResult = healthCheckResult;
    }

    public synchronized void add(Map<HostPort, HealthStatusDesc> result) {
        this.healthCheckResult.add(result);
    }

    public UpDownInstances aggregate(Set<HostPort> interested, int quorum) {
        Set<HostPort> healthyInstances = new HashSet<>();
        Set<HostPort> unhealthyInstances = new HashSet<>();

        interested.forEach(instance -> {
            List<HealthStatusDesc> statusList = getHealthStatus(instance);
            int upCnt = 0;
            int downCnt = 0;

            for (HealthStatusDesc status: statusList) {
                if (status.getState().shouldNotifyMarkup()) upCnt++;
                else if (status.getState().shouldNotifyMarkDown()) downCnt++;
            }

            if (upCnt >= quorum) healthyInstances.add(instance);
            else if (downCnt >= quorum) unhealthyInstances.add(instance);
        });

        return new UpDownInstances(healthyInstances, unhealthyInstances);
    }

    public List<HealthStatusDesc> getHealthStatus(HostPort hostPort) {
        List<HealthStatusDesc> statusList = new ArrayList<>();
        healthCheckResult.forEach(result -> {
            if (result.containsKey(hostPort)) statusList.add(result.get(hostPort));
        });
        return statusList;
    }

}
