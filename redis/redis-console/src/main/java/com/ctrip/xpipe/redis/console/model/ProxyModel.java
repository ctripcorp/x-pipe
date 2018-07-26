package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
public class ProxyModel {

    private String uri;

    private String dcName;

    private long id;

    private boolean active;

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

    public static ProxyModel fromProxyTbl(ProxyTbl proxyTbl, DcService dcService) {
        ProxyModel model = new ProxyModel();
        model = model.setActive(proxyTbl.isActive()).setUri(proxyTbl.getUri()).setId(proxyTbl.getId());
        String dcName = dcService.find(proxyTbl.getDcId()).getDcName();
        model.setDcName(dcName);
        return model;
    }

    public ProxyTbl toProxyTbl(DcService dcService) {
        ProxyTbl proto = new ProxyTbl();
        proto.setActive(active).setDcId(dcService.find(dcName).getId()).setId(id).setUri(uri);
        return proto;
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
}
