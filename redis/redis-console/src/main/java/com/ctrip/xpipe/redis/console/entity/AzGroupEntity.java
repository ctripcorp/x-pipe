package com.ctrip.xpipe.redis.console.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;

/**
 * <p>
 * 多个az组合的group，需在region之内
 * </p>
 *
 * @author mybatis-generator
 * @since 2023-08-26
 */
@TableName("az_group_tbl")
public class AzGroupEntity extends BaseEntity {

    private static final long serialVersionUID = 1L;

    /**
     * 主键id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * az group名称
     */
    @TableField("name")
    private String name;

    /**
     * az group所属region
     */
    @TableField("region")
    private String region;

    /**
     * 创建时间
     */
    @TableField(value = "create_time", fill = FieldFill.INSERT)
    private Date createTime;

    public Long getId() {
        return id;
    }

    public AzGroupEntity setId(Long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public AzGroupEntity setName(String name) {
        this.name = name;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public AzGroupEntity setRegion(String region) {
        this.region = region;
        return this;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public AzGroupEntity setCreateTime(Date createTime) {
        this.createTime = createTime;
        return this;
    }

    public static final String ID = "id";

    public static final String NAME = "name";

    public static final String REGION = "region";

    public static final String CREATE_TIME = "create_time";

    @Override
    public String toString() {
        return "AzGroupEntity{" + "id = " + id + ", name = " + name + ", region = " + region + ", createTime = "
            + createTime + "}";
    }
}
