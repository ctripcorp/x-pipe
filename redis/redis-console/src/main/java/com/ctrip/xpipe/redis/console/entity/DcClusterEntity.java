package com.ctrip.xpipe.redis.console.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * <p>
 * dc cluster base info
 * </p>
 *
 * @author mybatis-generator
 * @since 2023-07-11
 */
@TableName("dc_cluster_tbl")
public class DcClusterEntity extends BaseEntity {

    private static final long serialVersionUID = 1L;

    /**
     * primary key
     */
    @TableId(value = "dc_cluster_id", type = IdType.AUTO)
    private Long dcClusterId;

    /**
     * reference dc id
     */
    @TableField("dc_id")
    private Long dcId;

    /**
     * reference cluster id
     */
    @TableField("cluster_id")
    private Long clusterId;

    /**
     * 对应az_group_cluster id，可为0，表示不关联az_group
     */
    @TableField("az_group_cluster_id")
    private Long azGroupClusterId;

    /**
     * reference metaserver id
     */
    @TableField("metaserver_id")
    private Long metaserverId;

    /**
     * dc cluster phase
     */
    @TableField("dc_cluster_phase")
    private Integer dcClusterPhase;

    /**
     * active redis check rules
     */
    @TableField("active_redis_check_rules")
    private String activeRedisCheckRules;

    /**
     * reference group name, null means same as dc name
     */
    @TableField("group_name")
    private String groupName;

    /**
     * reference group type, 1 means DRMaster and 0 means Master
     */
    @TableField("group_type")
    private String groupType;

    public Long getDcClusterId() {
        return dcClusterId;
    }

    public DcClusterEntity setDcClusterId(Long dcClusterId) {
        this.dcClusterId = dcClusterId;
        return this;
    }

    public Long getDcId() {
        return dcId;
    }

    public DcClusterEntity setDcId(Long dcId) {
        this.dcId = dcId;
        return this;
    }

    public Long getClusterId() {
        return clusterId;
    }

    public DcClusterEntity setClusterId(Long clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public Long getAzGroupClusterId() {
        return azGroupClusterId;
    }

    public DcClusterEntity setAzGroupClusterId(Long azGroupClusterId) {
        this.azGroupClusterId = azGroupClusterId;
        return this;
    }

    public Long getMetaserverId() {
        return metaserverId;
    }

    public DcClusterEntity setMetaserverId(Long metaserverId) {
        this.metaserverId = metaserverId;
        return this;
    }

    public Integer getDcClusterPhase() {
        return dcClusterPhase;
    }

    public DcClusterEntity setDcClusterPhase(Integer dcClusterPhase) {
        this.dcClusterPhase = dcClusterPhase;
        return this;
    }

    public String getActiveRedisCheckRules() {
        return activeRedisCheckRules;
    }

    public DcClusterEntity setActiveRedisCheckRules(String activeRedisCheckRules) {
        this.activeRedisCheckRules = activeRedisCheckRules;
        return this;
    }

    public String getGroupName() {
        return groupName;
    }

    public DcClusterEntity setGroupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    public String getGroupType() {
        return groupType;
    }

    public DcClusterEntity setGroupType(String groupType) {
        this.groupType = groupType;
        return this;
    }

    public static final String DC_CLUSTER_ID = "dc_cluster_id";

    public static final String DC_ID = "dc_id";

    public static final String CLUSTER_ID = "cluster_id";

    public static final String AZ_GROUP_CLUSTER_ID = "az_group_cluster_id";

    public static final String METASERVER_ID = "metaserver_id";

    public static final String DC_CLUSTER_PHASE = "dc_cluster_phase";

    public static final String ACTIVE_REDIS_CHECK_RULES = "active_redis_check_rules";

    public static final String GROUP_NAME = "group_name";

    public static final String GROUP_TYPE = "group_type";

    @Override
    public String toString() {
        return "DcClusterEntity{" + "dcClusterId = " + dcClusterId + ", dcId = " + dcId + ", clusterId = " + clusterId
            + ", azGroupClusterId = " + azGroupClusterId + ", metaserverId = " + metaserverId + ", dcClusterPhase = "
            + dcClusterPhase + ", activeRedisCheckRules = " + activeRedisCheckRules + ", groupName = " + groupName
            + ", groupType = " + groupType + "}";
    }
}
