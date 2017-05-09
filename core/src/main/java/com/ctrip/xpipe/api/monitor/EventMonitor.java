package com.ctrip.xpipe.api.monitor;

import com.ctrip.xpipe.monitor.CatEventMonitor;

/**
 * @author leoliang
 *
 *         2017年3月1日
 */
public interface EventMonitor {

    EventMonitor DEFAULT = new CatEventMonitor();

    String ALERT_TYPE = "alert";

    void logEvent(String type, String name, long count);

    void logEvent(String type, String name);

    void logAlertEvent(String simpleAlertMessage);
}
