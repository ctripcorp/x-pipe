package com.ctrip.xpipe.redis.console.health.action;

import com.ctrip.xpipe.metric.HostPort;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
public class InstanceDown extends AbstractInstanceEvent{
    public InstanceDown(HostPort hostPort) {
        super(hostPort);
    }
}
