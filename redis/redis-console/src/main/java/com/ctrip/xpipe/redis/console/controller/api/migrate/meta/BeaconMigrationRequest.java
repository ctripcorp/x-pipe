package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import java.util.*;

/**
 * @author lishanglin
 * date 2020/12/28
 */
public class BeaconMigrationRequest {

    private String clusterName;

    private long clusterId;

    private List<String> failoverGroups;

    private List<String> recoverGroups;

    private List<GroupStatusVO> groups;

    private List<Map<String, Map<String, String>>> checkResults;

    private Map<String, String> extra;

    private Boolean isForced;

    private String targetIDC;

    private Set<String> availableDcs;

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public void setTargetIDC(String targetIDC) {
        this.targetIDC = targetIDC;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setFailoverGroups(List<String> failoverGroups) {
        this.failoverGroups = failoverGroups;
    }

    public void setRecoverGroups(List<String> recoverGroups) {
        this.recoverGroups = recoverGroups;
    }

    public void setGroups(List<GroupStatusVO> groups) {
        this.groups = groups;
    }

    public void setCheckResults(List<Map<String, Map<String, String>>> checkResults) {
        this.checkResults = checkResults;
    }

    public void setExtra(Map<String, String> extra) {
        this.extra = extra;
    }

    public void setForced(Boolean forced) {
        isForced = forced;
    }

    public String getClusterName() {
        return clusterName;
    }

    public long getClusterId() {
        return clusterId;
    }

    public List<String> getFailoverGroups() {
        return failoverGroups;
    }

    public List<String> getRecoverGroups() {
        return recoverGroups;
    }

    public List<GroupStatusVO> getGroups() {
        return groups;
    }

    public List<Map<String, Map<String, String>>> getCheckResults() {
        return checkResults;
    }

    public Map<String, String> getExtra() {
        return extra;
    }

    public Boolean getForced() {
        return isForced;
    }

    public String getTargetIDC() {
        return targetIDC;
    }

    public Set<String> getAvailableDcs() {
        if (null != this.availableDcs) return this.availableDcs;

        Set<String> availables = new HashSet<>();
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
            if (null != health && health) availables.add(dc);
        });

        this.availableDcs = availables;
        return this.availableDcs;
    }

    public Set<String> getFailDcs() {
        if (failoverGroups.isEmpty()) return Collections.emptySet();

        Set<String> failDcs = new HashSet<>();
        failoverGroups.forEach(groupName -> {
            String[] infos = groupName.split("\\+");
            failDcs.add(infos[1]);
        });

        return failDcs;
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
}
