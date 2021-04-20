package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;

import static com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService.BEACON_GROUP_SEPARATOR_REGEX;

/**
 * @author lishanglin
 * date 2020/12/28
 */
public class BeaconMigrationRequest {

    private String clusterName;

    private Set<String> failoverGroups;

    private Set<String> recoverGroups;

    private Set<MonitorGroupMeta> groups;

    private List<Map<String, Map<String, String>>> checkResults;

    private Map<String, String> extra;

    private Boolean isForced;

    private String targetIDC;

    private Set<String> availableDcs;

    @JsonIgnore
    private ClusterTbl clusterTbl;

    @JsonIgnore
    private MigrationClusterTbl currentMigrationCluster;

    @JsonIgnore
    private DcTbl sourceDcTbl;

    @JsonIgnore
    private DcTbl targetDcTbl;

    @JsonIgnore
    private long migrationEventId;

    public long getMigrationEventId() {
        return migrationEventId;
    }

    public void setMigrationEventId(long migrationEventId) {
        this.migrationEventId = migrationEventId;
    }

    public void setTargetIDC(String targetIDC) {
        this.targetIDC = targetIDC;
    }

    public DcTbl getSourceDcTbl() {
        return sourceDcTbl;
    }

    public void setSourceDcTbl(DcTbl sourceDcTbl) {
        this.sourceDcTbl = sourceDcTbl;
    }

    public DcTbl getTargetDcTbl() {
        return targetDcTbl;
    }

    public void setTargetDcTbl(DcTbl targetDcTbl) {
        this.targetDcTbl = targetDcTbl;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setFailoverGroups(Set<String> failoverGroups) {
        this.failoverGroups = failoverGroups;
    }

    public void setRecoverGroups(Set<String> recoverGroups) {
        this.recoverGroups = recoverGroups;
    }

    public void setGroups(Set<MonitorGroupMeta> groups) {
        this.groups = groups;
    }

    public void setCheckResults(List<Map<String, Map<String, String>>> checkResults) {
        this.checkResults = checkResults;
    }

    public void setExtra(Map<String, String> extra) {
        this.extra = extra;
    }

    public void setIsForced(Boolean forced) {
        isForced = forced;
    }

    public String getClusterName() {
        return clusterName;
    }

    public Set<String> getFailoverGroups() {
        return failoverGroups;
    }

    public Set<String> getRecoverGroups() {
        return recoverGroups;
    }

    public Set<MonitorGroupMeta> getGroups() {
        return groups;
    }

    public List<Map<String, Map<String, String>>> getCheckResults() {
        return checkResults;
    }

    public Map<String, String> getExtra() {
        return extra;
    }

    public boolean getIsForced() {
        return null != isForced && isForced;
    }

    public String getTargetIDC() {
        return targetIDC;
    }

    @VisibleForTesting
    public void setAvailableDcs(Set<String> availableDcs) {
        this.availableDcs = availableDcs;
    }

    @JsonIgnore
    public Set<String> getAvailableDcs() {
        if (null != this.availableDcs) return this.availableDcs;

        Set<String> localAvailableDcs = new HashSet<>();
        Map<String, Boolean> dcHealthMap = new HashMap<>();

        groups.forEach(group -> {
            String dc = group.getIdc();
            if (!dcHealthMap.containsKey(dc)) {
                dcHealthMap.put(dc, !group.getDown());
            } else {
                dcHealthMap.put(dc, dcHealthMap.get(dc) && (!group.getDown()));
            }
        });

        dcHealthMap.forEach((dc, health) -> {
            if (null != health && health) localAvailableDcs.add(dc);
        });

        this.availableDcs = localAvailableDcs;
        return this.availableDcs;
    }

    @JsonIgnore
    public Set<String> getFailDcs() {
        if (failoverGroups.isEmpty()) return Collections.emptySet();

        Set<String> failDcs = new HashSet<>();
        failoverGroups.forEach(groupName -> {
            String[] infos = groupName.split(BEACON_GROUP_SEPARATOR_REGEX);
            failDcs.add(infos[1]);
        });

        return failDcs;
    }

    public ClusterTbl getClusterTbl() {
        return clusterTbl;
    }

    public void setClusterTbl(ClusterTbl clusterTbl) {
        this.clusterTbl = clusterTbl;
    }

    public MigrationClusterTbl getCurrentMigrationCluster() {
        return currentMigrationCluster;
    }

    public void setCurrentMigrationCluster(MigrationClusterTbl currentMigrationCluster) {
        this.currentMigrationCluster = currentMigrationCluster;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BeaconMigrationRequest that = (BeaconMigrationRequest) o;
        return Objects.equals(clusterName, that.clusterName) &&
                Objects.equals(failoverGroups, that.failoverGroups) &&
                Objects.equals(recoverGroups, that.recoverGroups) &&
                Objects.equals(groups, that.groups) &&
                Objects.equals(checkResults, that.checkResults) &&
                Objects.equals(extra, that.extra) &&
                Objects.equals(isForced, that.isForced) &&
                Objects.equals(targetIDC, that.targetIDC);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterName, failoverGroups, recoverGroups, groups, checkResults, extra, isForced, targetIDC);
    }

    @Override
    public String toString() {
        return "BeaconMigrationRequest{" +
                "clusterName='" + clusterName + '\'' +
                ", failoverGroups=" + failoverGroups +
                ", recoverGroups=" + recoverGroups +
                ", groups=" + groups +
                ", checkResults=" + checkResults +
                ", extra=" + extra +
                ", isForced=" + isForced +
                ", targetIDC='" + targetIDC + '\'' +
                '}';
    }
}
