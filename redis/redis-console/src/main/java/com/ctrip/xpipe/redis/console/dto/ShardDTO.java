package com.ctrip.xpipe.redis.console.dto;

import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2023/11/22
 */
public class ShardDTO {

    private Long shardId;

    private String shardName;

    public ShardDTO() {

    }

    public ShardDTO(ShardTbl shardTbl) {
        this.shardId = shardTbl.getId();
        this.shardName = shardTbl.getShardName();
    }

    public ShardDTO(ShardMeta shardMeta) {
        this.shardId = shardMeta.getDbId();
        this.shardName = shardMeta.getId();
    }

    public Long getShardId() {
        return shardId;
    }

    public void setShardId(Long shardId) {
        this.shardId = shardId;
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
        ShardDTO shardDTO = (ShardDTO) o;
        return shardId.equals(shardDTO.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId);
    }

    @Override
    public String toString() {
        return "ShardDTO{" +
                "shardId=" + shardId +
                ", shardName='" + shardName + '\'' +
                '}';
    }
}
