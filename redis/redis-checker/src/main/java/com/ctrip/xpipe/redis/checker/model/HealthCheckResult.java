package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.tuple.Pair;

import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class HealthCheckResult {

    private Map<HostPort, Boolean> redisAlives;

    private Map<HostPort, Long> redisDelays;

    private Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> crossMasterDelays;

    private Map<String, Set<String>> warningClusterShards;

    public Map<HostPort, Boolean> getRedisAlives() {
        return redisAlives;
    }

    public void setRedisAlives(Map<HostPort, Boolean> redisAlives) {
        this.redisAlives = redisAlives;
    }

    public Map<HostPort, Long> getRedisDelays() {
        return redisDelays;
    }

    public void setRedisDelays(Map<HostPort, Long> redisDelays) {
        this.redisDelays = redisDelays;
    }

    public Map<String, Set<String>> getWarningClusterShards() {
        return warningClusterShards;
    }

    public void setWarningClusterShards(Map<String, Set<String>> warningClusterShards) {
        this.warningClusterShards = warningClusterShards;
    }

    public Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> getCrossMasterDelays() {
        return crossMasterDelays;
    }

    public void setCrossMasterDelays(Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> crossMasterDelays) {
        this.crossMasterDelays = crossMasterDelays;
    }

    @Override
    public String toString() {
        return "HealthCheckResult{" +
                "redisAlives=" + redisAlives +
                ", redisDelays=" + redisDelays +
                ", crossMasterDelays=" + crossMasterDelays +
                ", warningClusterShards=" + warningClusterShards +
                '}';
    }
}
