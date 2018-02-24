package com.ctrip.xpipe.redis.console.notifier.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */
public abstract class AbstractClusterEventFactory {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public abstract ClusterEvent createClusterEvent(String clusterName);
}
