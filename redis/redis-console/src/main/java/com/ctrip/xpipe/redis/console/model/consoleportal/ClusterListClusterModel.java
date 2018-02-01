package com.ctrip.xpipe.redis.console.model.consoleportal;

/**
 * @author chen.zhu
 * <p>
 * Jan 31, 2018
 */
public class ClusterListClusterModel extends AbstractClusterModel {

    private Long activedcId;

    private String message;

    public ClusterListClusterModel(String clusterName, String clusterAdminEmails,
                                   String clusterDescription, String clusterOrgName,
                                   Long activedcId, String message) {
        super(clusterName, clusterAdminEmails, clusterDescription, clusterOrgName);
        this.activedcId = activedcId;
        this.message = message;
    }

    public ClusterListClusterModel(String clusterName) {
        super(clusterName);
    }

    public ClusterListClusterModel() {
    }

    public Long getActivedcId() {
        return activedcId;
    }

    public ClusterListClusterModel setActivedcId(Long activedcId) {
        this.activedcId = activedcId;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public ClusterListClusterModel setMessage(String message) {
        this.message = message;
        return this;
    }
}
