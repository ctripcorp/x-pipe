package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */

public class ProxyModel implements Serializable {

    private String uri;

    private String dcName;

    private long id;

    private boolean active;

    private boolean monitorActive;

    private HostPort hostPort;

    public String getUri() {
        return uri;
    }

    public ProxyModel setUri(String uri) {
        this.uri = uri;
        return this;
    }

    public String getDcName() {
        return dcName;
    }

    public ProxyModel setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public long getId() {
        return id;
    }

    public ProxyModel setId(long id) {
        this.id = id;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public ProxyModel setActive(boolean active) {
        this.active = active;
        return this;
    }

    public ProxyModel setHostPort(HostPort hostPort) {
        this.hostPort = hostPort;
        return this;
    }

    public static ProxyModel fromProxyTbl(ProxyTbl proxyTbl, DcIdNameMapper mapper) {
        ProxyModel model = new ProxyModel();
        model = model.setActive(proxyTbl.isActive()).setUri(proxyTbl.getUri()).setId(proxyTbl.getId())
                .setMonitorActive(proxyTbl.isMonitorActive());
        model.setDcName(mapper.getName(proxyTbl.getDcId()));
        return model;
    }

    public ProxyTbl toProxyTbl(DcIdNameMapper mapper) {
        ProxyTbl proto = new ProxyTbl();
        proto.setActive(active).setId(id).setUri(uri).setMonitorActive(monitorActive);
        proto.setDcId(mapper.getId(dcName));
        return proto;
    }

    public HostPort getHostPort() {
        if(hostPort == null) {
            Endpoint endpoint = new DefaultProxyEndpoint(uri);
            hostPort = new HostPort(endpoint.getHost(), endpoint.getPort());
        }
        return hostPort;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProxyModel that = (ProxyModel) o;
        return id == that.id &&
                active == that.active &&
                Objects.equals(uri, that.uri) &&
                Objects.equals(dcName, that.dcName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, dcName, id, active);
    }

    @Override
    public ProxyModel clone() {
        ProxyModel clone = new ProxyModel();
        clone.uri = this.uri;
        clone.dcName = this.dcName;
        clone.id = this.id;
        clone.active = this.active;
        clone.monitorActive = this.monitorActive;
        if (null != this.hostPort) clone.hostPort = new HostPort(this.hostPort.getHost(), this.hostPort.getPort());
        return clone;
    }

    @Override
    public String toString() {
        return String.format("ProxyModel[uri: %s, active: %b, dc-name: %s, id: %d]", uri, active, dcName, id);
    }

    public boolean isMonitorActive() {
        return monitorActive;
    }

    public ProxyModel setMonitorActive(boolean monitorActive) {
        this.monitorActive = monitorActive;
        return this;
    }
}
