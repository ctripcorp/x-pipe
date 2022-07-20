package com.ctrip.xpipe.redis.core.metaserver;

/**
 * @author lishanglin
 * date 2022/7/20
 */
public class MetaserverAddress {

    private String dcName;

    private String address;

    public MetaserverAddress(String dcName, String address) {
        this.dcName = dcName;
        this.address = address;
    }

    public String getDcName() {
        return dcName;
    }

    public String getAddress() {
        return address;
    }
}
