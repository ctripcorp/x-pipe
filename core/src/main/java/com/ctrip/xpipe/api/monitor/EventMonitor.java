package com.ctrip.xpipe.api.monitor;

import com.ctrip.xpipe.monitor.CatEventMonitor;

/**
 * @author leoliang
 *
 *         2017年3月1日
 */
public interface EventMonitor {
    public static EventMonitor DEFAULT = new CatEventMonitor();

    void logEvent(String type, String name, long count);
}
