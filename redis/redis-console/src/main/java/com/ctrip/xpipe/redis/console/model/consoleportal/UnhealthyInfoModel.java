package com.ctrip.xpipe.redis.console.model.consoleportal;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.StringUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.*;

public class UnhealthyInfoModel {

    private int unhealthyCluster;

    private int unhealthyShard;

    private int unhealthyRedis;

    private List<String> attachFailDc;

    @JsonDeserialize(contentUsing = ClusterUnhealthyInfoDeserializer.class)
    private Map<String, Map<DcShard, Set<RedisHostPort>>> unhealthyInstance;

    public UnhealthyInfoModel() {
        this.attachFailDc = new ArrayList<>();
        this.unhealthyInstance = new HashMap<>();
    }

    public UnhealthyInfoModel merge(UnhealthyInfoModel other) {
        if (null == other) return this;

        other.unhealthyInstance.forEach((cluster, shards) -> {
            shards.forEach((dcShardName, instances) -> {
                instances.forEach(redis -> {
                    this.addUnhealthyInstance(cluster, dcShardName, redis);
                });
            });
        });

        return this;
    }

    public void addUnhealthyInstance(String cluster, String dc, String shard, HostPort redis, boolean isMaster) {
        addUnhealthyInstance(cluster, new DcShard(dc, shard), new RedisHostPort(redis, isMaster));
    }

    private void addUnhealthyInstance(String cluster, DcShard dcShard, RedisHostPort redis) {
        if (!unhealthyInstance.containsKey(cluster)) {
            unhealthyCluster++;
            this.unhealthyInstance.put(cluster, new HashMap<>());
        }

        if (!unhealthyInstance.get(cluster).containsKey(dcShard)) {
            unhealthyShard++;
            this.unhealthyInstance.get(cluster).put(dcShard, new HashSet<>());
        }

        if (this.unhealthyInstance.get(cluster).get(dcShard).add(redis)) {
            unhealthyRedis++;
        }
    }

    @JsonIgnore
    public Set<String> getUnhealthyClusterNames() {
        return this.unhealthyInstance.keySet();
    }

    public Set<DcShard> getUnhealthyDcShardByCluster(String clusterName) {
        if (null == clusterName || !this.unhealthyInstance.containsKey(clusterName)) return Collections.emptySet();
        return unhealthyInstance.get(clusterName).keySet();
    }

    public List<String> getUnhealthyClusterDesc(String clusterName) {
        if (null == clusterName || !this.unhealthyInstance.containsKey(clusterName)) return Collections.emptyList();
        List<String> messages = new ArrayList<>();

        for (Map.Entry<DcShard, Set<RedisHostPort> > shard : unhealthyInstance.get(clusterName).entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append(shard.getKey()).append(":");
            for (RedisHostPort redis : shard.getValue()) {
                sb.append(redis).append(",");
            }

            sb.append(";");
            messages.add(sb.toString());
        }

        return messages;
    }

    public int countUnhealthyShardByCluster(String clusterName) {
        if (null == clusterName || !this.unhealthyInstance.containsKey(clusterName)) return 0;
        return unhealthyInstance.get(clusterName).size();
    }

    public int countUnhealthyRedisByCluster(String clusterName) {
        if (null == clusterName || !this.unhealthyInstance.containsKey(clusterName)) return 0;
        return unhealthyInstance.get(clusterName).values().stream().mapToInt(Set::size).sum();
    }

    public int getUnhealthyCluster() {
        return unhealthyCluster;
    }

    public int getUnhealthyShard() {
        return unhealthyShard;
    }

    public int getUnhealthyRedis() {
        return unhealthyRedis;
    }

    public List<String> getAttachFailDc() {
        return attachFailDc;
    }

    public void setAttachFailDc(List<String> attachFailDc) {
        this.attachFailDc = attachFailDc;
    }

    public Map<String, Map<DcShard, Set<RedisHostPort>>> getUnhealthyInstance() {
        return unhealthyInstance;
    }

    public void setUnhealthyInstance(Map<String, Map<DcShard, Set<RedisHostPort>>> unhealthyInstance) {
        this.unhealthyInstance = unhealthyInstance;
    }

    public void accept(UnhealthyInstanceConsumer unhealthyInstanceConsumer) {
        if (null == this.unhealthyInstance || this.unhealthyInstance.isEmpty()) return;

        this.unhealthyInstance.forEach((cluster, dcShards) -> {
            dcShards.forEach((dcShard, instances) -> {
                instances.forEach(instance -> {
                    unhealthyInstanceConsumer.consume(dcShard.getDc(), cluster, dcShard.getShard(), instance.getHostPort(), instance.isMaster());
                });
            });
        });
    }

    @FunctionalInterface
    public interface UnhealthyInstanceConsumer {

        void consume(String dc, String cluster, String shard, HostPort hostPort, boolean isMaster);

    }

    public static class RedisHostPort {

        private HostPort hostPort;

        private boolean isMaster;

        public RedisHostPort() {

        }

        public RedisHostPort(HostPort hostPort, boolean isMaster) {
            this.hostPort = hostPort;
            this.isMaster = isMaster;
        }

        public HostPort getHostPort() {
            return hostPort;
        }

        public void setHostPort(HostPort hostPort) {
            this.hostPort = hostPort;
        }

        public boolean isMaster() {
            return isMaster;
        }

        public void setMaster(boolean master) {
            isMaster = master;
        }

        @Override
        public String toString() {
            return String.format("%s %s", hostPort, isMaster ? "master" : "slave");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RedisHostPort that = (RedisHostPort) o;
            return isMaster == that.isMaster &&
                    Objects.equals(hostPort, that.hostPort);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hostPort, isMaster);
        }
    }

    public static class DcShard {

        private String dc;

        private String shard;

        public DcShard(String raw) {
            if (StringUtil.isEmpty(raw)) {
                return;
            }
            String[] infos = raw.split(" ");
            if (infos.length >= 2) {
                this.dc = infos[0];
                this.shard = infos[1];
            }
        }

        public DcShard(String dc, String shard) {
            this.dc = dc;
            this.shard = shard;
        }

        public String getDc() {
            return dc;
        }

        public void setDc(String dc) {
            this.dc = dc;
        }

        public String getShard() {
            return shard;
        }

        public void setShard(String shard) {
            this.shard = shard;
        }

        @Override
        public String toString() {
            return String.format("%s %s", dc, shard);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DcShard dcShard = (DcShard) o;
            return Objects.equals(dc, dcShard.dc) &&
                    Objects.equals(shard, dcShard.shard);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dc, shard);
        }
    }

    public static class ClusterUnhealthyInfoDeserializer extends StdDeserializer<Map<DcShard, Set<RedisHostPort>>> {

        private static ObjectMapper objectMapper = new ObjectMapper();

        private static TypeReference<Map<DcShard, Set<RedisHostPort>>> typeRef
                = new TypeReference<Map<DcShard, Set<RedisHostPort>>>() {};

        public ClusterUnhealthyInfoDeserializer() {
            this(null);
        }

        public ClusterUnhealthyInfoDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public Map<DcShard, Set<RedisHostPort>> deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            return objectMapper.readValue(jp, typeRef);
        }
    }

}
