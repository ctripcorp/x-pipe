package com.ctrip.xpipe.redis.core.util;

import com.ctrip.xpipe.tuple.Pair;

public class SentinelUtil {

    protected final static char SENTINEL_MONITOR_NAME_CLUE = '+';

    public static String getSentinelMonitorName(String monitorName, String idc) {
        return monitorName + SENTINEL_MONITOR_NAME_CLUE + idc;
    }

    public static Pair<String, String> getSentinelMonitorDcAndName(String sentinelMonitor) {
        int index = sentinelMonitor.lastIndexOf(SENTINEL_MONITOR_NAME_CLUE);
        if (index == -1 || index > sentinelMonitor.length() - 2) {
            return new Pair<>("", sentinelMonitor);
        }
        return new Pair<>(sentinelMonitor.substring(index + 1), sentinelMonitor.substring(0, index));
    }
}
