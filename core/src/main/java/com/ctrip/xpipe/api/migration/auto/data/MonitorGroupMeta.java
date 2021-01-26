package com.ctrip.xpipe.api.migration.auto.data;

import com.ctrip.xpipe.endpoint.HostPort;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.util.Converter;
import com.fasterxml.jackson.databind.util.StdConverter;

import java.util.Objects;
import java.util.Set;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public class MonitorGroupMeta {

    public MonitorGroupMeta() {

    }

    public MonitorGroupMeta(String name, String idc, Set<HostPort> nodes, boolean masterGroup) {
        this.name = name;
        this.idc = idc;
        this.nodes = nodes;
        this.masterGroup = masterGroup;
    }

    private String name;

    private String idc;

    private Set<HostPort> nodes;

    private Boolean down;

    private boolean masterGroup;

    public String getName() {
        return name;
    }

    public String getIdc() {
        return idc;
    }

    public Boolean getDown() {
        return down;
    }

    @JsonSerialize(contentConverter = NodeSerializeConverter.class)
    public Set<HostPort> getNodes() {
        return nodes;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setIdc(String idc) {
        this.idc = idc;
    }

    public void setDown(Boolean down) {
        this.down = down;
    }

    public void setNodes(Set<HostPort> nodes) {
        this.nodes = nodes;
    }

    public boolean isMasterGroup() {
        return masterGroup;
    }

    public void setMasterGroup(boolean masterGroup) {
        this.masterGroup = masterGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MonitorGroupMeta that = (MonitorGroupMeta) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(idc, that.idc) &&
                Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, idc, nodes);
    }

    @Override
    public String toString() {
        return "BeaconGroupModel{" +
                "name='" + name + '\'' +
                ", idc='" + idc + '\'' +
                ", nodes=" + nodes +
                ", down=" + down +
                ", masterGroup=" + masterGroup +
                '}';
    }

    public static class NodeSerializeConverter extends StdConverter<HostPort, String> implements Converter<HostPort, String> {

        @Override
        public String convert(HostPort addr) {
            if (null == addr) return null;
            return addr.toString();
        }

    }
}
