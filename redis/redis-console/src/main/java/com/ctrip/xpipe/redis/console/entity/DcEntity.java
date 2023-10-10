package com.ctrip.xpipe.redis.console.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * <p>
 * dc base info
 * </p>
 *
 * @author mybatis-generator
 * @since 2023-07-17
 */
@TableName("dc_tbl")
public class DcEntity extends BaseEntity {

    private static final long serialVersionUID = 1L;

    /**
     * primary key
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * zone id
     */
    @TableField("zone_id")
    private Long zoneId;

    /**
     * dc name
     */
    @TableField("dc_name")
    private String dcName;

    /**
     * dc active status
     */
    @TableField("dc_active")
    private Byte dcActive;

    /**
     * dc description
     */
    @TableField("dc_description")
    private String dcDescription;

    /**
     * last modified tag
     */
    @TableField("dc_last_modified_time")
    private String dcLastModifiedTime;

    public Long getId() {
        return id;
    }

    public DcEntity setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getZoneId() {
        return zoneId;
    }

    public DcEntity setZoneId(Long zoneId) {
        this.zoneId = zoneId;
        return this;
    }

    public String getDcName() {
        return dcName;
    }

    public DcEntity setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public Byte getDcActive() {
        return dcActive;
    }

    public DcEntity setDcActive(Byte dcActive) {
        this.dcActive = dcActive;
        return this;
    }

    public String getDcDescription() {
        return dcDescription;
    }

    public DcEntity setDcDescription(String dcDescription) {
        this.dcDescription = dcDescription;
        return this;
    }

    public String getDcLastModifiedTime() {
        return dcLastModifiedTime;
    }

    public DcEntity setDcLastModifiedTime(String dcLastModifiedTime) {
        this.dcLastModifiedTime = dcLastModifiedTime;
        return this;
    }

    public static final String ID = "id";

    public static final String ZONE_ID = "zone_id";

    public static final String DC_NAME = "dc_name";

    public static final String DC_ACTIVE = "dc_active";

    public static final String DC_DESCRIPTION = "dc_description";

    public static final String DC_LAST_MODIFIED_TIME = "dc_last_modified_time";

    @Override
    public String toString() {
        return "DcEntity{" + "id = " + id + ", zoneId = " + zoneId + ", dcName = " + dcName + ", dcActive = " + dcActive
            + ", dcDescription = " + dcDescription + ", dcLastModifiedTime = " + dcLastModifiedTime + "}";
    }
}
