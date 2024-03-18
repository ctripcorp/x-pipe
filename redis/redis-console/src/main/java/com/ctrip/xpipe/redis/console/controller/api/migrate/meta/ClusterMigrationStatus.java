package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;

import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2024/3/18
 */
public class ClusterMigrationStatus {

    public Long startAt;

    public Long endAt;

    public String sourceDc;

    public String destDc;

    public String status;

    public static ClusterMigrationStatus from(MigrationClusterTbl migrationClusterTbl, DcCache dcCache) {
        ClusterMigrationStatus migrationStatus = new ClusterMigrationStatus();
        migrationStatus.startAt = TimeUnit.MILLISECONDS.toSeconds(migrationClusterTbl.getStartTime().getTime());
        if (null != migrationClusterTbl.getEndTime()) {
            migrationStatus.endAt = TimeUnit.MILLISECONDS.toSeconds(migrationClusterTbl.getEndTime().getTime());
        } else {
            migrationStatus.endAt = null;
        }

        DcTbl srcDcTbl = dcCache.find(migrationClusterTbl.getSourceDcId());
        DcTbl destDcTbl = dcCache.find(migrationClusterTbl.getDestinationDcId());
        if (null != srcDcTbl) migrationStatus.sourceDc = srcDcTbl.getDcName();
        if (null != destDcTbl) migrationStatus.destDc = destDcTbl.getDcName();
        migrationStatus.status = MigrationStatus.valueOf(migrationClusterTbl.getStatus()).getType();

        return migrationStatus;
    }

    @Override
    public String toString() {
        return "ClusterMigrationStatus{" +
                "startAt=" + startAt +
                ", endAt=" + endAt +
                ", sourceDc='" + sourceDc + '\'' +
                ", destDc='" + destDc + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
