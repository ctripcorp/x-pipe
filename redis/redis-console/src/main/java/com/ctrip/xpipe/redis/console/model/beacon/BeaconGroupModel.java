package com.ctrip.xpipe.redis.console.model.beacon;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Objects;
import java.util.Set;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public class BeaconGroupModel {

    public BeaconGroupModel() {

    }

    public BeaconGroupModel(String name, String idc, Set<HostPort> nodes, boolean masterGroup) {
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
        BeaconGroupModel that = (BeaconGroupModel) o;
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
}
