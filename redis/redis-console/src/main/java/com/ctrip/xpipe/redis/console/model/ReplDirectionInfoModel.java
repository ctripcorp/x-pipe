package com.ctrip.xpipe.redis.console.model;

import java.io.Serializable;

public class ReplDirectionInfoModel implements Serializable {

    private static final long serialVersionUID = 1L;

    private long id;

    private String clusterName;

    private String srcDcName;


    private String fromDcName;

    private String toDcName;

    public ReplDirectionInfoModel() {

    }

    public long getId() {
        return id;
    }

    public ReplDirectionInfoModel setId(long id) {
        this.id = id;
        return this;
    }

    public String getClusterName() {
        return clusterName;
    }

    public ReplDirectionInfoModel setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getSrcDcName() {
        return srcDcName;
    }

    public ReplDirectionInfoModel setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
        return this;
    }

    public String getFromDcName() {
        return fromDcName;
    }

    public ReplDirectionInfoModel setFromDcName(String fromDcName) {
        this.fromDcName = fromDcName;
        return this;
    }

    public String getToDcName() {
        return toDcName;
    }

    public ReplDirectionInfoModel setToDcName(String toDcName) {
        this.toDcName = toDcName;
        return this;
    }

    @Override
    public String toString() {
        return "ReplDirectionInfoModel{" +
                "id=" + id +
                ", clusterName='" + clusterName + '\'' +
                ", srcDcName='" + srcDcName + '\'' +
                ", fromDcName='" + fromDcName + '\'' +
                ", toDcName='" + toDcName + '\'' +
                '}';
    }
}
