package com.ctrip.xpipe.redis.console.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;

/**
 * <p>
 * az与az_group对应关系表
 * </p>
 *
 * @author mybatis-generator
 * @since 2023-07-10
 */
@TableName("az_group_mapping_tbl")
public class AzGroupMappingEntity extends BaseEntity {

    private static final long serialVersionUID = 1L;

    /**
     * 主键id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 目前对应dc id，后续迁移dc至az
     */
    @TableField("az_id")
    private Long azId;

    /**
     * 对应az_group_id
     */
    @TableField("az_group_id")
    private Long azGroupId;

    /**
     * 创建时间
     */
    @TableField(value = "create_time", fill = FieldFill.INSERT)
    private Date createTime;

    public Long getId() {
        return id;
    }

    public AzGroupMappingEntity setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getAzId() {
        return azId;
    }

    public AzGroupMappingEntity setAzId(Long azId) {
        this.azId = azId;
        return this;
    }

    public Long getAzGroupId() {
        return azGroupId;
    }

    public AzGroupMappingEntity setAzGroupId(Long azGroupId) {
        this.azGroupId = azGroupId;
        return this;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public AzGroupMappingEntity setCreateTime(Date createTime) {
        this.createTime = createTime;
        return this;
    }

    public static final String ID = "id";

    public static final String AZ_ID = "az_id";

    public static final String AZ_GROUP_ID = "az_group_id";

    public static final String CREATE_TIME = "create_time";

    @Override
    public String toString() {
        return "AzGroupMappingEntity{" + "id = " + id + ", azId = " + azId + ", azGroupId = " + azGroupId
            + ", createTime = " + createTime + "}";
    }
}
