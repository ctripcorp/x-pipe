package com.ctrip.xpipe.redis.console.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;

/**
 * <p>
 * cluster与az_group关联关系表
 * </p>
 *
 * @author mybatis-generator
 * @since 2023-11-14
 */
@TableName("az_group_cluster_tbl")
public class AzGroupClusterEntity extends BaseEntity {

    private static final long serialVersionUID = 1L;

    /**
     * 主键id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 对应cluster id
     */
    @TableField("cluster_id")
    private Long clusterId;

    /**
     * 对应az_group id
     */
    @TableField("az_group_id")
    private Long azGroupId;

    /**
     * 此az group中的主az对应id
     */
    @TableField("active_az_id")
    private Long activeAzId;

    /**
     * 此az group中的集群类型
     */
    @TableField("az_group_cluster_type")
    private String azGroupClusterType;

    /**
     * 创建时间
     */
    @TableField(value = "create_time", fill = FieldFill.INSERT)
    private Date createTime;

    /**
     * 逻辑删除时间
     */
    @TableField("delete_time")
    private Date deleteTime;

    public Long getId() {
        return id;
    }

    public AzGroupClusterEntity setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getClusterId() {
        return clusterId;
    }

    public AzGroupClusterEntity setClusterId(Long clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public Long getAzGroupId() {
        return azGroupId;
    }

    public AzGroupClusterEntity setAzGroupId(Long azGroupId) {
        this.azGroupId = azGroupId;
        return this;
    }

    public Long getActiveAzId() {
        return activeAzId;
    }

    public AzGroupClusterEntity setActiveAzId(Long activeAzId) {
        this.activeAzId = activeAzId;
        return this;
    }

    public String getAzGroupClusterType() {
        return azGroupClusterType;
    }

    public AzGroupClusterEntity setAzGroupClusterType(String azGroupClusterType) {
        this.azGroupClusterType = azGroupClusterType;
        return this;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public AzGroupClusterEntity setCreateTime(Date createTime) {
        this.createTime = createTime;
        return this;
    }

    public Date getDeleteTime() {
        return deleteTime;
    }

    public AzGroupClusterEntity setDeleteTime(Date deleteTime) {
        this.deleteTime = deleteTime;
        return this;
    }

    public static final String ID = "id";

    public static final String CLUSTER_ID = "cluster_id";

    public static final String AZ_GROUP_ID = "az_group_id";

    public static final String ACTIVE_AZ_ID = "active_az_id";

    public static final String AZ_GROUP_CLUSTER_TYPE = "az_group_cluster_type";

    public static final String CREATE_TIME = "create_time";

    public static final String DELETE_TIME = "delete_time";

    @Override
    public String toString() {
        return "AzGroupClusterEntity{" + "id = " + id + ", clusterId = " + clusterId + ", azGroupId = " + azGroupId
            + ", activeAzId = " + activeAzId + ", azGroupClusterType = " + azGroupClusterType + ", createTime = "
            + createTime + ", deleteTime = " + deleteTime + "}";
    }
}
