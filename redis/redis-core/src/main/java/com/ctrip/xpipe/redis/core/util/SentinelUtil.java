package com.ctrip.xpipe.redis.core.util;

import com.ctrip.xpipe.utils.StringUtil;

public class SentinelUtil {

    protected final static char SENTINEL_MONITOR_NAME_CLUE = '+';

    public static String getSentinelMonitorName(String clusterName, String monitorName, String idc) {
        return clusterName + SENTINEL_MONITOR_NAME_CLUE + monitorName + SENTINEL_MONITOR_NAME_CLUE + idc;
    }

    public static SentinelInfo getSentinelInfoFromMonitorName(String monitorName) {
        return SentinelInfo.fromMonitorName(monitorName);
    }

    public static class SentinelInfo {
        private String clusterName;
        private String shardName;
        private String idc;

        public SentinelInfo(String clusterName, String shardName, String idc) {
            this.clusterName = clusterName;
            this.shardName = shardName;
            this.idc = idc;
        }

        public static SentinelInfo fromMonitorName(String monitorName) {
            String[] strs = StringUtil.splitRemoveEmpty("\\+", monitorName);
            String shardName = strs.length > 1 ? strs[strs.length - 2] : monitorName;
            String idc = strs.length > 1 ? strs[strs.length - 1] : "";
            String clusterName = strs.length > 2 ? strs[0] : "";
            return new SentinelInfo(clusterName, shardName, idc);
        }

        public String getClusterName() {
            return clusterName;
        }

        public String getShardName() {
            return shardName;
        }

        public String getIdc() {
            return idc;
        }

        public boolean isAvailable() {
            return !StringUtil.isEmpty(clusterName) && !StringUtil.isEmpty(shardName) && !StringUtil.isEmpty(idc);
        }

    }
}
