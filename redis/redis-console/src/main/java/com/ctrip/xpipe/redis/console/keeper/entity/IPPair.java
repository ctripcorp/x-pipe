package com.ctrip.xpipe.redis.console.keeper.entity;

import java.util.Objects;

public class IPPair {

    private String ip1;
    private String ip2;

    public IPPair(String ip1, String ip2) {
        this.ip1 = ip1;
        this.ip2 = ip2;
    }

    public String getIp1() {
        return ip1;
    }

    public String getIp2() {
        return ip2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IPPair ipPair = (IPPair) o;
        return Objects.equals(ip1, ipPair.ip1) &&
                Objects.equals(ip2, ipPair.ip2) ||
                Objects.equals(ip1, ipPair.ip2) &&
                        Objects.equals(ip2, ipPair.ip1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip1, ip2) + Objects.hash(ip2, ip1);
    }

}
