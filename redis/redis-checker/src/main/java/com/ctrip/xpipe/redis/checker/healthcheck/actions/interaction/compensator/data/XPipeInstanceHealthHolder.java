package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;
import org.springframework.lang.Nullable;

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

    public @Nullable Boolean aggregate(HostPort instance, int quorum) {
        List<HealthStatusDesc> statusList = getHealthStatus(instance);
        int upCnt = 0;
        int downCnt = 0;

        for (HealthStatusDesc status: statusList) {
            if (status.getState().shouldNotifyMarkup()) upCnt++;
            else if (status.getState().shouldNotifyMarkDown()) downCnt++;
        }

        if (upCnt >= quorum) return Boolean.TRUE;
        else if (downCnt >= quorum) return Boolean.FALSE;
        else return null;
    }

    public UpDownInstances aggregate(Map<String, Set<HostPort>> interested, int quorum) {
        Set<HostPort> healthyInstances = new HashSet<>();
        Set<HostPort> unhealthyInstances = new HashSet<>();

        interested.values().forEach(instances -> {
            instances.forEach(instance -> {
                Boolean healthy = aggregate(instance, quorum);
                if (Boolean.TRUE.equals(healthy)) healthyInstances.add(instance);
                else if (Boolean.FALSE.equals(healthy)) unhealthyInstances.add(instance);
            });
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
