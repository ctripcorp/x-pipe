package com.ctrip.xpipe.redis.console.model.consoleportal;

/**
 * @author chen.zhu
 * <p>
 * Jan 31, 2018
 */
public class AbstractClusterModel {

    protected String clusterName;

    protected String clusterAdminEmails;

    protected String clusterDescription;

    protected String clusterOrgName;

    public AbstractClusterModel(String clusterName, String clusterAdminEmails,
                                String clusterDescription, String clusterOrgName) {
        this.clusterName = clusterName;
        this.clusterAdminEmails = clusterAdminEmails;
        this.clusterDescription = clusterDescription;
        this.clusterOrgName = clusterOrgName;
    }

    public AbstractClusterModel(String clusterName) {
        this.clusterName = clusterName;
    }

    public AbstractClusterModel() {

    }

    public String getClusterName() {
        return clusterName;
    }

    public AbstractClusterModel setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getClusterAdminEmails() {
        return clusterAdminEmails;
    }

    public AbstractClusterModel setClusterAdminEmails(String clusterAdminEmails) {
        this.clusterAdminEmails = clusterAdminEmails;
        return this;
    }

    public String getClusterDescription() {
        return clusterDescription;
    }

    public AbstractClusterModel setClusterDescription(String clusterDescription) {
        this.clusterDescription = clusterDescription;
        return this;
    }

    public String getClusterOrgName() {
        return clusterOrgName;
    }

    public AbstractClusterModel setClusterOrgName(String clusterOrgName) {
        this.clusterOrgName = clusterOrgName;
        return this;
    }
}
