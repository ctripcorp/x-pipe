package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.redis.console.dto.ClusterDTO;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2023/11/22
 */
public class ClusterInfo {

    private long id;

    private String clusterName;

    private String clusterType;

    private Set<ShardInfo> shards;

    public ClusterInfo() {

    }

    public ClusterInfo(ClusterDTO clusterDTO) {
        this.id = clusterDTO.getClusterId();
        this.clusterName = clusterDTO.getClusterName();
        this.clusterType = clusterDTO.getClusterType();
        if (null != clusterDTO.getShards()) {
            this.shards = clusterDTO.getShards().stream().map(ShardInfo::new).collect(Collectors.toSet());
        }
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterType() {
        return clusterType;
    }

    public void setClusterType(String clusterType) {
        this.clusterType = clusterType;
    }

    public Set<ShardInfo> getShards() {
        return shards;
    }

    public void setShards(Set<ShardInfo> shards) {
        this.shards = shards;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterInfo that = (ClusterInfo) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
