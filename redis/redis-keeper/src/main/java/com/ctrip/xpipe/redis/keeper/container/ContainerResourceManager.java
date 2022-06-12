package com.ctrip.xpipe.redis.keeper.container;

import com.google.common.collect.Sets;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * @author lishanglin
 * date 2022/6/9
 */
@Component
public class ContainerResourceManager {

    private Set<Integer> runningPorts = Sets.newConcurrentHashSet();

    public boolean isPortFree(int port) {
        return runningPorts.contains(port);
    }

    public boolean applyPort(int port) {
        return runningPorts.add(port);
    }

    public boolean releasePort(int port) {
        return runningPorts.remove(port);
    }

}
