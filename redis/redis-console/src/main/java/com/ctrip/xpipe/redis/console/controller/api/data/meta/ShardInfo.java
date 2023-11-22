package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.redis.console.dto.ShardDTO;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2023/11/22
 */
public class ShardInfo {

    private long id;

    private String shardName;

    public ShardInfo() {

    }

    public ShardInfo(ShardDTO shardDTO) {
        this.id = shardDTO.getShardId();
        this.shardName = shardDTO.getShardName();
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
