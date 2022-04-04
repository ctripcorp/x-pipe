package com.ctrip.xpipe.redis.core.protocal.pojo;


import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.Map;


public class DefaultSentinelMasterInstance extends AbstractSentinelRedisInstance implements SentinelMasterInstance {

    public DefaultSentinelMasterInstance(Map<String, String> infos) {
        super(infos);
    }


    @Override
    public String toString() {
        return "DefaultSentinelSlaveInstance{" +
                ", hostPort=" + hostPort +
                ", flags=" + (info == null ? "null" : info.get("flags")) +
                '}';
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(hostPort.getHost(), hostPort.getPort());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DefaultSentinelMasterInstance)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!ObjectUtils.equals(((DefaultSentinelMasterInstance) obj).getHostPort(), this.getHostPort())) {
            return false;
        }
        return true;
    }
}
