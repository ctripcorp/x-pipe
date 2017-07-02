package com.ctrip.xpipe.redis.console.service.migration.impl;


import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 29, 2017
 */
public class TryMigrateResult {

    private String clusterName;
    private long   clusterId;

    long fromDcId;
    String fromDcName;

    long toDcId;
    String toDcName;

    public TryMigrateResult(){

    }

    public TryMigrateResult(ClusterTbl clusterTbl, DcTbl fromDc, DcTbl toDc){

        this.clusterId = clusterTbl.getId();
        this.clusterName = clusterTbl.getClusterName();
        this.fromDcId = fromDc.getId();
        this.fromDcName = fromDc.getDcName();
        this.toDcId = toDc.getId();
        this.toDcName = toDc.getDcName();

    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public long getClusterId() {
        return clusterId;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public long getFromDcId() {
        return fromDcId;
    }

    public void setFromDcId(long fromDcId) {
        this.fromDcId = fromDcId;
    }

    public String getFromDcName() {
        return fromDcName;
    }

    public void setFromDcName(String fromDcName) {
        this.fromDcName = fromDcName;
    }

    public long getToDcId() {
        return toDcId;
    }

    public void setToDcId(long toDcId) {
        this.toDcId = toDcId;
    }

    public String getToDcName() {
        return toDcName;
    }

    public void setToDcName(String toDcName) {
        this.toDcName = toDcName;
    }

    @Override
    public String toString() {
        return String.format("%s, %s->%s", clusterName, fromDcName, toDcName);
    }
}
