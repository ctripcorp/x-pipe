package com.ctrip.xpipe.redis.console.model;

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
}
