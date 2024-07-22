package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.redis.console.entity.MigrationBiClusterEntity;

import java.util.Date;

/**
 * @author lishanglin
 * date 2024/7/17
 */
public class BiMigrationRecord {

    private long id;

    private String clusterName;

    private String operator;

    private Date operationTime;

    private String publishInfo;

    private String status;

    public static BiMigrationRecord fromMigrationBiClusterEntity(MigrationBiClusterEntity entity) {
        BiMigrationRecord record = new BiMigrationRecord();
        record.setId(entity.getId());
        record.setClusterName(null);
        record.setOperationTime(entity.getOperationTime());
        record.setOperator(entity.getOperator());
        record.setPublishInfo(entity.getPublishInfo());
        record.setStatus(entity.getStatus());
        return record;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public Date getOperationTime() {
        return operationTime;
    }

    public void setOperationTime(Date operationTime) {
        this.operationTime = operationTime;
    }

    public String getPublishInfo() {
        return publishInfo;
    }

    public void setPublishInfo(String publishInfo) {
        this.publishInfo = publishInfo;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

}
