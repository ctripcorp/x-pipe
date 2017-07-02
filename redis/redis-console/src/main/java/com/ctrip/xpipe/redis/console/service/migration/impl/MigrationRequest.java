package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 29, 2017
 */
public class MigrationRequest {

    private String user;

    private String tag;

    private List<ClusterInfo> requestClusters = new LinkedList<>();

    public MigrationRequest(String user) {
        this.user = user;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }

    public String getUser() {
        return user;
    }

    public List<ClusterInfo> getRequestClusters() {
        return new LinkedList<>(requestClusters);
    }

    public synchronized void addClusterInfo(ClusterInfo requestCluster) {

        for (ClusterInfo clusterInfo : requestClusters) {

            if (clusterInfo.getClusterId() == requestCluster.getClusterId()) {
                throw new IllegalStateException("[addClusterInfo][cluster already exist]" + clusterInfo);
            }
        }
        requestClusters.add(requestCluster);
    }

    public static class ClusterInfo {

        private long clusterId;
        private String clusterName;

        private long fromDcId;
        private String fromDcName;

        private long toDcId;
        private String toDcName;

        public ClusterInfo(){

        }

        public ClusterInfo(long clusterId, String clusterName, long fromDcId, String fromDcName, long toDcId, String toDcName){
            this.clusterId = clusterId;
            this.clusterName = clusterName;
            this.fromDcId = fromDcId;
            this.fromDcName = fromDcName;
            this.toDcId = toDcId;
            this.toDcName = toDcName;
        }

        public ClusterInfo(TryMigrateResult tryMigrateResult){
            this.clusterId = tryMigrateResult.getClusterId();
            this.clusterName = tryMigrateResult.getClusterName();
            this.fromDcId = tryMigrateResult.getFromDcId();
            this.fromDcName = tryMigrateResult.getFromDcName();
            this.toDcId = tryMigrateResult.getToDcId();
            this.toDcName = tryMigrateResult.getToDcName();
        }

        public ClusterInfo(MigrationClusterTbl migrationCluster){
            this.clusterId = migrationCluster.getClusterId();
            this.clusterName = migrationCluster.getCluster() == null ? "": migrationCluster.getCluster().getClusterName();

            this.fromDcId = migrationCluster.getSourceDcId();
            this.toDcId = migrationCluster.getDestinationDcId();
        }


        public long getClusterId() {
            return clusterId;
        }

        public void setClusterId(long clusterId) {
            this.clusterId = clusterId;
        }

        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
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
            return String.format("%s,%s->%s", clusterName, fromDcName, toDcName);
        }
    }


}
