package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class MigrationModel implements Serializable {
    private static final long serialVersionUID = 1L;

    private static transient Logger logger = LoggerFactory.getLogger(MigrationModel.class);

    private MigrationEventTbl event;

    private String status;

    // names only
    private List<String> clusters;

    public MigrationModel() {}

    public static MigrationModel createFromMigrationClusters(List<MigrationClusterTbl> clusters) {
        MigrationModel modal = new MigrationModel();
        if (clusters.isEmpty()) return modal;

        modal.setEvent(clusters.get(0).getMigrationEvent());
        modal.setClusters(clusters.stream()
                .map(migrationCluster -> migrationCluster.getCluster().getClusterName())
                .collect(Collectors.toList()));
        modal.setStatus(collectStatus(clusters.stream()
                .map(MigrationClusterTbl::getStatus)
                .collect(Collectors.toList())));

        return modal;
    }

    private static String collectStatus(List<String> statusList) {
        if (null == statusList || statusList.isEmpty()) return MigrationStatus.TYPE_SUCCESS;

        String curStatus;
        String targetStatus = null;
        for (String statusStr: statusList) {
            if (StringUtil.isEmpty(statusStr)) continue;
            try {
                MigrationStatus status = MigrationStatus.valueOf(statusStr);
                curStatus = status.getType();
                targetStatus = null == targetStatus ? curStatus : mergeStatus(targetStatus, curStatus);
                if (targetStatus.equals(MigrationStatus.TYPE_WARNING)) break;
            } catch (Exception e) {
                logger.warn("console - collect migration status error {}, status {}", e.getMessage(), statusStr);
            }
        }

        return targetStatus;
    }

    private static String mergeStatus(String left, String right) {
        if (left.equals(right)) return right;
        if (right.equals(MigrationStatus.TYPE_FAIL))
            return MigrationStatus.TYPE_WARNING;
        return MigrationStatus.TYPE_PROCESSING;
    }

    public MigrationEventTbl getEvent() {
        return event;
    }

    public void setEvent(MigrationEventTbl event) {
        this.event = event;
    }

    public void setClusters(List<String> clusters) {
        this.clusters = clusters;
    }

    public List<String> getClusters() {
        return this.clusters;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
