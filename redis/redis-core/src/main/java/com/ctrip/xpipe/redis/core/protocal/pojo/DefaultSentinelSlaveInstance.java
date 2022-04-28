package com.ctrip.xpipe.redis.core.protocal.pojo;


import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.Map;


public class DefaultSentinelSlaveInstance extends AbstractSentinelRedisInstance implements SentinelSlaveInstance {

    private HostPort masterInstance;

    public DefaultSentinelSlaveInstance(Map<String, String> infos) {
        super(infos);
        this.masterInstance = new HostPort(info.get("master-host"), Integer.parseInt(info.get("master-port")));
    }

    @Override
    public HostPort getMaster() {
        return masterInstance;
    }

    @Override
    public String toString() {
        return "DefaultSentinelSlaveInstance{" +
                "masterInstance=" + masterInstance +
                ", endpoint=" + hostPort +
                ", flags=" + (info == null ? "null" : info.get("flags")) +
                '}';
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(hostPort.getHost(), hostPort.getPort());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DefaultSentinelSlaveInstance)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        return ObjectUtils.equals(((DefaultSentinelSlaveInstance) obj).getHostPort(),hostPort);
    }
}
