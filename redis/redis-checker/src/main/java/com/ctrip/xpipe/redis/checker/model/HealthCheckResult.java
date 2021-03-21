package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.tuple.Pair;

import java.util.*;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class HealthCheckResult {

    private List<RedisAlive> redisAlives;

    private List<RedisDelay> redisDelays;

    private List<CrossMasterDelay> crossMasterDelays;

    private Map<String, Set<String>> warningClusterShards;

    public List<RedisAlive> getRedisAlives() {
        return this.redisAlives;
    }

    public Map<HostPort, Boolean> decodeRedisAlives() {
        Map<HostPort, Boolean> result = new HashMap<>();
        if (null != this.redisAlives) redisAlives.forEach(redisAlive -> result.put(redisAlive.hostPort, redisAlive.alive));
        return result;
    }

    public void encodeRedisAlives(Map<HostPort, Boolean> redisAlives) {
        List<RedisAlive> localRedisAlives = new ArrayList<>();
        redisAlives.forEach(((hostPort, alive) -> localRedisAlives.add(new RedisAlive(hostPort, alive))));
        this.redisAlives = localRedisAlives;
    }

    public Map<HostPort, Long> decodeRedisDelays() {
        Map<HostPort, Long> result = new HashMap<>();
        if (null != this.redisDelays) redisDelays.forEach(redisDelay -> result.put(redisDelay.hostPort, redisDelay.delay));
        return result;
    }

    public List<RedisDelay> getRedisDelays() {
        return this.redisDelays;
    }

    public void encodeRedisDelays(Map<HostPort, Long> redisDelays) {
        List<RedisDelay> localRedisDelay = new ArrayList<>();
        redisDelays.forEach(((hostPort, delay) -> localRedisDelay.add(new RedisDelay(hostPort, delay))));
        this.redisDelays = localRedisDelay;
    }

    public Map<String, Set<String>> getWarningClusterShards() {
        return warningClusterShards;
    }

    public void setWarningClusterShards(Map<String, Set<String>> warningClusterShards) {
        this.warningClusterShards = warningClusterShards;
    }

    public Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> decodeCrossMasterDelays() {
        Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> result = new HashMap<>();
        if (null != this.crossMasterDelays) this.crossMasterDelays.forEach(
                crossMastersDelay -> result.put(crossMastersDelay.dcClusterShard, crossMastersDelay.delays));
        return result;
    }

    public List<CrossMasterDelay> getCrossMasterDelays() {
        return this.crossMasterDelays;
    }

    public void encodeCrossMasterDelays(Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> crossMasterDelays) {
        List<CrossMasterDelay> localCrossMasterDelays = new ArrayList<>();
        crossMasterDelays.forEach(((dcClusterShard, delays) -> localCrossMasterDelays.add(new CrossMasterDelay(dcClusterShard, delays))));
        this.crossMasterDelays = localCrossMasterDelays;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HealthCheckResult result = (HealthCheckResult) o;
        return Objects.equals(redisAlives, result.redisAlives) &&
                Objects.equals(redisDelays, result.redisDelays) &&
                Objects.equals(crossMasterDelays, result.crossMasterDelays) &&
                Objects.equals(warningClusterShards, result.warningClusterShards);
    }

    @Override
    public int hashCode() {
        return Objects.hash(redisAlives, redisDelays, crossMasterDelays, warningClusterShards);
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

    // inner class for serialize as json, otherwise we need to use object as map key
    public static class RedisDelay {

        public RedisDelay() {

        }

        public RedisDelay(HostPort hostPort, Long delay) {
            this.hostPort = hostPort;
            this.delay = delay;
        }

        public HostPort hostPort;

        public Long delay;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RedisDelay that = (RedisDelay) o;
            return Objects.equals(hostPort, that.hostPort) &&
                    Objects.equals(delay, that.delay);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hostPort, delay);
        }

        @Override
        public String toString() {
            return "RedisDelay{" +
                    "hostPort=" + hostPort +
                    ", delay=" + delay +
                    '}';
        }
    }

    public static class RedisAlive {

        public RedisAlive() {

        }

        public RedisAlive(HostPort hostPort, Boolean alive) {
            this.hostPort = hostPort;
            this.alive = alive;
        }

        public HostPort hostPort;

        public Boolean alive;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RedisAlive that = (RedisAlive) o;
            return Objects.equals(hostPort, that.hostPort) &&
                    Objects.equals(alive, that.alive);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hostPort, alive);
        }

        @Override
        public String toString() {
            return "RedisAlive{" +
                    "hostPort=" + hostPort +
                    ", alive=" + alive +
                    '}';
        }
    }

    public static class CrossMasterDelay {

        public CrossMasterDelay() {

        }

        public CrossMasterDelay(DcClusterShard dcClusterShard, Map<String, Pair<HostPort, Long>> delays) {
            this.dcClusterShard = dcClusterShard;
            this.delays = delays;
        }

        public DcClusterShard dcClusterShard;

        public Map<String, Pair<HostPort, Long>> delays;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CrossMasterDelay that = (CrossMasterDelay) o;
            return Objects.equals(dcClusterShard, that.dcClusterShard) &&
                    Objects.equals(delays, that.delays);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dcClusterShard, delays);
        }

        @Override
        public String toString() {
            return "CrossMasterDelay{" +
                    "dcClusterShard=" + dcClusterShard +
                    ", delays=" + delays +
                    '}';
        }
    }

}
