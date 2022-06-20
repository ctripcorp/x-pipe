package com.ctrip.xpipe.redis.console.model;

import java.util.List;

public class DcClusterModel implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private DcClusterTbl dcCluster;

    private DcModel dc;

    private List<ShardModel> shards;

    private List<SourceModel> sources;

    public DcClusterModel() {
    }

    public DcClusterTbl getDcCluster() {
        return dcCluster;
    }

    public DcClusterModel setDcCluster(DcClusterTbl dcCluster) {
        this.dcCluster = dcCluster;
        return this;
    }

    public DcModel getDc() {
        return dc;
    }

    public DcClusterModel setDc(DcModel dc) {
        this.dc = dc;
        return this;
    }

    public List<ShardModel> getShards() {
        return shards;
    }

    public DcClusterModel setShards(List<ShardModel> shards) {
        this.shards = shards;
        return this;
    }

    public List<SourceModel> getSources() {
        return sources;
    }

    public DcClusterModel setSources(List<SourceModel> sources) {
        this.sources = sources;
        return this;
    }

    @Override
    public String toString() {
        return "DcClusterModel{" +
                "dcCluster=" + dcCluster +
                ", dc=" + dc +
                ", shards=" + shards +
                ", sources=" + sources +
                '}';
    }
}
