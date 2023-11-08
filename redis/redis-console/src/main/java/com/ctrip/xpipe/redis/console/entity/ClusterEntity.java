package com.ctrip.xpipe.redis.console.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.util.Date;

/**
 * <p>
 * clusters info
 * </p>
 *
 * @author mybatis-generator
 * @since 2023-11-06
 */
@TableName("cluster_tbl")
public class ClusterEntity extends BaseEntity {

    private static final long serialVersionUID = 1L;

    /**
     * primary key
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * cluster name
     */
    @TableField("cluster_name")
    private String clusterName;

    /**
     * cluster type
     */
    @TableField("cluster_type")
    private String clusterType;

    /**
     * active dc id
     */
    @TableField("activedc_id")
    private Long activedcId;

    /**
     * cluster description
     */
    @TableField("cluster_description")
    private String clusterDescription;

    /**
     * last modified tag
     */
    @TableField("cluster_last_modified_time")
    private String clusterLastModifiedTime;

    /**
     * cluster status
     */
    @TableField("status")
    private String status;

    /**
     * related migration event on processing
     */
    @TableField("migration_event_id")
    private Long migrationEventId;

    /**
     * is xpipe interested
     */
    @TableField("is_xpipe_interested")
    private Byte isXpipeInterested;

    /**
     * organization id of cluster
     */
    @TableField("cluster_org_id")
    private Long clusterOrgId;

    /**
     * persons email who in charge of this cluster
     */
    @TableField("cluster_admin_emails")
    private String clusterAdminEmails;

    /**
     * designated routeIds
     */
    @TableField("cluster_designated_route_ids")
    private String clusterDesignatedRouteIds;

    /**
     * cluster create time
     */
    @TableField(value = "create_time", fill = FieldFill.INSERT)
    private Date createTime;

    public Long getId() {
        return id;
    }

    public ClusterEntity setId(Long id) {
        this.id = id;
        return this;
    }

    public String getClusterName() {
        return clusterName;
    }

    public ClusterEntity setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getClusterType() {
        return clusterType;
    }

    public ClusterEntity setClusterType(String clusterType) {
        this.clusterType = clusterType;
        return this;
    }

    public Long getActivedcId() {
        return activedcId;
    }

    public ClusterEntity setActivedcId(Long activedcId) {
        this.activedcId = activedcId;
        return this;
    }

    public String getClusterDescription() {
        return clusterDescription;
    }

    public ClusterEntity setClusterDescription(String clusterDescription) {
        this.clusterDescription = clusterDescription;
        return this;
    }

    public String getClusterLastModifiedTime() {
        return clusterLastModifiedTime;
    }

    public ClusterEntity setClusterLastModifiedTime(String clusterLastModifiedTime) {
        this.clusterLastModifiedTime = clusterLastModifiedTime;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public ClusterEntity setStatus(String status) {
        this.status = status;
        return this;
    }

    public Long getMigrationEventId() {
        return migrationEventId;
    }

    public ClusterEntity setMigrationEventId(Long migrationEventId) {
        this.migrationEventId = migrationEventId;
        return this;
    }

    public Byte getIsXpipeInterested() {
        return isXpipeInterested;
    }

    public ClusterEntity setIsXpipeInterested(Byte isXpipeInterested) {
        this.isXpipeInterested = isXpipeInterested;
        return this;
    }

    public Long getClusterOrgId() {
        return clusterOrgId;
    }

    public ClusterEntity setClusterOrgId(Long clusterOrgId) {
        this.clusterOrgId = clusterOrgId;
        return this;
    }

    public String getClusterAdminEmails() {
        return clusterAdminEmails;
    }

    public ClusterEntity setClusterAdminEmails(String clusterAdminEmails) {
        this.clusterAdminEmails = clusterAdminEmails;
        return this;
    }

    public String getClusterDesignatedRouteIds() {
        return clusterDesignatedRouteIds;
    }

    public ClusterEntity setClusterDesignatedRouteIds(String clusterDesignatedRouteIds) {
        this.clusterDesignatedRouteIds = clusterDesignatedRouteIds;
        return this;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public ClusterEntity setCreateTime(Date createTime) {
        this.createTime = createTime;
        return this;
    }

    public static final String ID = "id";

    public static final String CLUSTER_NAME = "cluster_name";

    public static final String CLUSTER_TYPE = "cluster_type";

    public static final String ACTIVEDC_ID = "activedc_id";

    public static final String CLUSTER_DESCRIPTION = "cluster_description";

    public static final String CLUSTER_LAST_MODIFIED_TIME = "cluster_last_modified_time";

    public static final String STATUS = "status";

    public static final String MIGRATION_EVENT_ID = "migration_event_id";

    public static final String IS_XPIPE_INTERESTED = "is_xpipe_interested";

    public static final String CLUSTER_ORG_ID = "cluster_org_id";

    public static final String CLUSTER_ADMIN_EMAILS = "cluster_admin_emails";

    public static final String CLUSTER_DESIGNATED_ROUTE_IDS = "cluster_designated_route_ids";

    public static final String CREATE_TIME = "create_time";

    @Override
    public String toString() {
        return "ClusterEntity{" + "id = " + id + ", clusterName = " + clusterName + ", clusterType = " + clusterType
            + ", activedcId = " + activedcId + ", clusterDescription = " + clusterDescription
            + ", clusterLastModifiedTime = " + clusterLastModifiedTime + ", status = " + status
            + ", migrationEventId = " + migrationEventId + ", isXpipeInterested = " + isXpipeInterested
            + ", clusterOrgId = " + clusterOrgId + ", clusterAdminEmails = " + clusterAdminEmails
            + ", clusterDesignatedRouteIds = " + clusterDesignatedRouteIds + ", createTime = " + createTime + "}";
    }
}
