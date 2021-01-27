package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/17
 */
public class TooManyNeedExcludeClusterException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    private Set<String> needExcludeClusters;

    public TooManyNeedExcludeClusterException(Set<String> needExcludeClusters) {
        super(String.format("too many cluster to be exclude: %s", needExcludeClusters.toString()));
        this.needExcludeClusters = needExcludeClusters;
    }

    public Set<String> getNeedExcludeClusters() {
        return needExcludeClusters;
    }
}
