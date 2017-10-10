package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

import java.util.Date;
import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 02, 2017
 */
public class MigrationClusterInfo {

    private long id;

    private String clusterName;

    private long migrationEventId;

    private long clusterId;

    private String  sourceDcName;

    private String destinationDcName;

    private java.util.Date startTime;

    private java.util.Date endTime;

    private String status;

    private String statusType;

    private boolean isEnd;

    private String publishInfo;

    public MigrationClusterInfo(String clusterName, Map<Long, String> dcNameMap, MigrationClusterTbl migrationClusterTbl){
        this.id = migrationClusterTbl.getId();
        this.clusterName = clusterName;
        this.migrationEventId = migrationClusterTbl.getMigrationEventId();
        this.clusterId = migrationClusterTbl.getClusterId();
        this.sourceDcName = dcNameMap.get(migrationClusterTbl.getSourceDcId());
        this.destinationDcName = dcNameMap.get(migrationClusterTbl.getDestinationDcId());
        this.startTime = migrationClusterTbl.getStartTime();
        this.endTime = migrationClusterTbl.getEndTime();
        this.status = migrationClusterTbl.getStatus();
        MigrationStatus migrationStatus = MigrationStatus.valueOf(status);
        this.statusType = migrationStatus.getType();
        this.isEnd = migrationStatus.isTerminated();
        this.publishInfo = migrationClusterTbl.getPublishInfo();
    }

    public String getClusterName() {
        return clusterName;
    }

    public long getMigrationEventId() {
        return migrationEventId;
    }

    public long getClusterId() {
        return clusterId;
    }

    public String getSourceDcName() {
        return sourceDcName;
    }

    public String getDestinationDcName() {
        return destinationDcName;
    }

    public Date getStartTime() {
        return startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public String getStatus() {
        return status;
    }

    public String getPublishInfo() {
        return publishInfo;
    }

    public long getId() {
        return id;
    }

    public String getStatusType() {
        return statusType;
    }

    public boolean isEnd() {
        return isEnd;
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
