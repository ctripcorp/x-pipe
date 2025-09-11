package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.entity.ClusterEntity;
import com.ctrip.xpipe.redis.console.entity.MigrationBiClusterEntity;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ClusterMigrationStatusV2 {

    public Long startAt;

    public Long endAt;

    public String clusterType;

    public Set<String> sourceDcs;

    public Set<String> destDcs;

    public String status;

    public static ClusterMigrationStatusV2 from(ClusterEntity cluster, MigrationClusterTbl migrationClusterTbl, DcCache dcCache) {
        ClusterMigrationStatusV2 migrationStatus = new ClusterMigrationStatusV2();
        migrationStatus.clusterType = ClusterType.lookup(cluster.getClusterType()).name();
        migrationStatus.startAt = TimeUnit.MILLISECONDS.toSeconds(migrationClusterTbl.getStartTime().getTime());
        if (null != migrationClusterTbl.getEndTime()) {
            migrationStatus.endAt = TimeUnit.MILLISECONDS.toSeconds(migrationClusterTbl.getEndTime().getTime());
        } else {
            migrationStatus.endAt = null;
        }

        DcTbl srcDcTbl = dcCache.find(migrationClusterTbl.getSourceDcId());
        DcTbl destDcTbl = dcCache.find(migrationClusterTbl.getDestinationDcId());
        if (null != srcDcTbl) migrationStatus.sourceDcs = Collections.singleton(srcDcTbl.getDcName());
        if (null != destDcTbl) migrationStatus.destDcs = Collections.singleton(destDcTbl.getDcName());
        migrationStatus.status = MigrationStatus.valueOf(migrationClusterTbl.getStatus()).getType();

        return migrationStatus;
    }

    private static GenericTypeReference<Set<String>> stringSetType = new GenericTypeReference<Set<String>>(){};
    public static ClusterMigrationStatusV2 from(ClusterEntity cluster, MigrationBiClusterEntity biMigrationRecord, Set<String> relatedDcs) {
        ClusterMigrationStatusV2 migrationStatus = new ClusterMigrationStatusV2();
        migrationStatus.clusterType = ClusterType.lookup(cluster.getClusterType()).name();
        migrationStatus.startAt = TimeUnit.MILLISECONDS.toSeconds(biMigrationRecord.getOperationTime().getTime());
        migrationStatus.endAt = TimeUnit.MILLISECONDS.toSeconds(biMigrationRecord.getOperationTime().getTime());

        Set<String> srcDcs = relatedDcs;
        Set<String> destDcs = new HashSet<>(srcDcs);
        Set<String> excludedDcs = Codec.DEFAULT.decode(biMigrationRecord.getPublishInfo(), stringSetType);
        destDcs.removeAll(excludedDcs);

        migrationStatus.sourceDcs = srcDcs;
        migrationStatus.destDcs = destDcs;
        migrationStatus.status = biMigrationRecord.getStatus();

        return migrationStatus;
    }

    @Override
    public String toString() {
        return "ClusterMigrationStatusV2{" +
                "startAt=" + startAt +
                ", endAt=" + endAt +
                ", clusterType='" + clusterType + '\'' +
                ", sourceDcs=" + sourceDcs +
                ", destDcs=" + destDcs +
                ", status='" + status + '\'' +
                '}';
    }
}
