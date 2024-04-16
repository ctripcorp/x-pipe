package com.ctrip.xpipe.api.monitor;

import com.ctrip.xpipe.monitor.CatEventMonitor;

import java.util.Map;

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

    void logEvent(String type, String name, Map<String, String> nameValuePairs);

    void logAlertEvent(String simpleAlertMessage);
}
