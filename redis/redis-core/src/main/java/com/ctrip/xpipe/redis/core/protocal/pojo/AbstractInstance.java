package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.ObjectUtils;


public abstract class AbstractInstance implements Instance {

    protected HostPort hostPort;

    public AbstractInstance(HostPort hostPort) {
        this.hostPort = hostPort;
    }

    protected AbstractInstance() {
    }

    @Override
    public HostPort getHostPort() {
        return hostPort;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(hostPort);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AbstractInstance)) {
            return false;
        }
        return ObjectUtils.equals(obj, hostPort);
    }
}
