package com.ctrip.xpipe.redis.console.controller.api.data.meta;

public class DcClusterCreateInfo extends AbstractCreateInfo{
    private String clusterName;

    private String dcName;

    private String redisConfigRule;

    @Override
    public void check() throws CheckFailException {

    }

    @Override
    public String toString() {
        return "DcClusterCreateInfo{" +
                "clusterName='" + clusterName + '\'' +
                ", dcName='" + dcName + '\'' +
                ", redisConfigRule='" + redisConfigRule + '\'' +
                '}';
    }

    public String getClusterName() {
        return clusterName;
    }

    public DcClusterCreateInfo setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getDcName() {
        return dcName;
    }

    public DcClusterCreateInfo setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public String getRedisConfigRule() {
        return redisConfigRule;
    }

    public DcClusterCreateInfo setRedisConfigRule(String redisConfigRule) {
        this.redisConfigRule = redisConfigRule;
        return this;
    }
}
