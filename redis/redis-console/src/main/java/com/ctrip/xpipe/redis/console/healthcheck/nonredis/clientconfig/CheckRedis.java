package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clientconfig;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
public class CheckRedis extends OuterClientService.AbstractInfo{

    private HostPort hostPort;
    private String idc;

    public CheckRedis(String host, int port, String idc) {
        this.hostPort = new HostPort(host, port);
        this.idc = idc;
    }

    public void setHostPort(HostPort hostPort) {
        this.hostPort = hostPort;
    }

    public void setIdc(String idc) {
        this.idc = idc;
    }

    public HostPort getHostPort() {
        return hostPort;
    }

    public String getIdc() {
        return idc;
    }

    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof CheckRedis)) {
            throw new EqualsException("obj not redis:" + obj);
        }

        CheckRedis other = (CheckRedis) obj;
        if (!(ObjectUtils.equals(hostPort, other.hostPort))) {
            throw new EqualsException(String.format("hostport not equal: %s %s", hostPort, other.hostPort));
        }
        if (!(ObjectUtils.equals(idc, other.idc))) {
            throw new EqualsException(String.format("idc not equal: %s %s", idc, other.idc));
        }

        return true;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(hostPort, idc);
    }

}
