package com.ctrip.xpipe.redis.console.health.action;


import com.ctrip.xpipe.metric.HostPort;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
public class AbstractInstanceEvent {

    protected HostPort hostPort;

    public AbstractInstanceEvent(HostPort hostPort){
        this.hostPort = hostPort;
    }


    public HostPort getHostPort() {
        return hostPort;
    }


    @Override
    public String toString() {
        return String.format("%s:%s", getHostPort(), getClass().getSimpleName());
    }
}
