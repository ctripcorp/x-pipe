package com.ctrip.xpipe.redis.proxy.controller;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * Jun 01, 2018
 */
public class ComponentRegistryHolder {

    private static AtomicBoolean isRegistrySet = new AtomicBoolean(false);
    private static ComponentRegistry componentRegistry;

    public static void initializeRegistry(ComponentRegistry componentRegistryToSet) {
        if (isRegistrySet.compareAndSet(false, true)) {
            componentRegistry = componentRegistryToSet;
        }
    }

    public static ComponentRegistry getComponentRegistry() {
        return componentRegistry;
    }
}
