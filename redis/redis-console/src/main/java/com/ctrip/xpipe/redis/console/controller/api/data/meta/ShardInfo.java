package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.redis.console.dto.ShardDTO;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author lishanglin
 * date 2023/11/22
 */
public class ShardInfo {

    private long id;

    private String shardName;

    private Set<InstanceInfo> redises = new HashSet<>();

    private Set<InstanceInfo> keepers = new HashSet<>();

    public ShardInfo() {

    }

    public ShardInfo(ShardDTO shardDTO) {
        this.id = shardDTO.getShardId();
        this.shardName = shardDTO.getShardName();
        this.redises = shardDTO.getRedises();
        this.keepers = shardDTO.getKeepers();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public Set<InstanceInfo> getKeepers() {
        return keepers;
    }

    public void setKeepers(Set<InstanceInfo> keepers) {
        this.keepers = keepers;
    }

    public Set<InstanceInfo> getRedises() {
        return redises;
    }

    public void setRedises(Set<InstanceInfo> redises) {
        this.redises = redises;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardInfo shardInfo = (ShardInfo) o;
        return id == shardInfo.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
