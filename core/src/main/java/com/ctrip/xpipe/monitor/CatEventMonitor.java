package com.ctrip.xpipe.monitor;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;

/**
 * @author leoliang
 *
 *         2017年3月1日
 */
public class CatEventMonitor implements EventMonitor {

    @Override
    public void logEvent(String type, String name, long count) {
        Cat.logEvent(type, name, Event.SUCCESS, "*count=" + count);
    }

    @Override
    public void logEvent(String type, String name) {
        Cat.logEvent(type, name);
    }

}
