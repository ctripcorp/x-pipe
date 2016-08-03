package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class ComponentRegistryHolder {
    private static AtomicBoolean isRegistrySet = new AtomicBoolean(false);
    private static ComponentRegistry componentRegistry;

    public static void initializeRegistry(ComponentRegistry componentRegistryToSet) {
        if (!isRegistrySet.compareAndSet(false, true)) {
            //already set
            return;
        }
        componentRegistry = componentRegistryToSet;
    }

    public static ComponentRegistry getComponentRegistry() {
        return componentRegistry;
    }
}
