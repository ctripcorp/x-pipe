package com.ctrip.xpipe.api.migration.auto.data;

import java.util.List;
import java.util.Objects;

public class MonitorShardMeta {

    private String name;

    private List<MonitorGroupMeta> groups;

    public MonitorShardMeta() {
    }

    public MonitorShardMeta(String name, List<MonitorGroupMeta> groups) {
        this.name = name;
        this.groups = groups;
    }

    public String getName() {
        return name;
    }

    public List<MonitorGroupMeta> getGroups() {
        return groups;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setGroups(List<MonitorGroupMeta> groups) {
        this.groups = groups;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MonitorShardMeta that = (MonitorShardMeta) o;
        return Objects.equals(name, that.name) && Objects.equals(groups, that.groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, groups);
    }
}
