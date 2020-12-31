package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.List;
import java.util.Objects;

/**
 * @author lishanglin
 * date 2020/12/28
 */
public class GroupStatusVO {

    private String name;

    private String idc;

    private Boolean down;

    private List<HostPort> nodes;

    public String getName() {
        return name;
    }

    public String getIdc() {
        return idc;
    }

    public Boolean getDown() {
        return down;
    }

    public List<HostPort> getNodes() {
        return nodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupStatusVO that = (GroupStatusVO) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(idc, that.idc) &&
                Objects.equals(down, that.down) &&
                Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, idc, down, nodes);
    }

}
