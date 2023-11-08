package com.ctrip.xpipe.redis.console.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * <p>
 * shard base info
 * </p>
 *
 * @author mybatis-generator
 * @since 2023-11-06
 */
@TableName("shard_tbl")
public class ShardEntity extends BaseEntity {

    private static final long serialVersionUID = 1L;

    /**
     * primary key
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * shard name
     */
    @TableField("shard_name")
    private String shardName;

    /**
     * reference cluster id
     */
    @TableField("cluster_id")
    private Long clusterId;

    /**
     * az group cluster id
     */
    @TableField("az_group_cluster_id")
    private Long azGroupClusterId;

    /**
     * setinel monitor name
     */
    @TableField("setinel_monitor_name")
    private String setinelMonitorName;

    public Long getId() {
        return id;
    }

    public ShardEntity setId(Long id) {
        this.id = id;
        return this;
    }

    public String getShardName() {
        return shardName;
    }

    public ShardEntity setShardName(String shardName) {
        this.shardName = shardName;
        return this;
    }

    public Long getClusterId() {
        return clusterId;
    }

    public ShardEntity setClusterId(Long clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public Long getAzGroupClusterId() {
        return azGroupClusterId;
    }

    public ShardEntity setAzGroupClusterId(Long azGroupClusterId) {
        this.azGroupClusterId = azGroupClusterId;
        return this;
    }

    public String getSetinelMonitorName() {
        return setinelMonitorName;
    }

    public ShardEntity setSetinelMonitorName(String setinelMonitorName) {
        this.setinelMonitorName = setinelMonitorName;
        return this;
    }

    public static final String ID = "id";

    public static final String SHARD_NAME = "shard_name";

    public static final String CLUSTER_ID = "cluster_id";

    public static final String AZ_GROUP_CLUSTER_ID = "az_group_cluster_id";

    public static final String SETINEL_MONITOR_NAME = "setinel_monitor_name";

    @Override
    public String toString() {
        return "ShardEntity{" + "id = " + id + ", shardName = " + shardName + ", clusterId = " + clusterId
            + ", azGroupClusterId = " + azGroupClusterId + ", setinelMonitorName = " + setinelMonitorName + "}";
    }
}
