package com.ctrip.xpipe.redis.checker.healthcheck.allleader.sentinel;

import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;

import java.util.List;

public class SentinelMonitors {
    private static final String COLON_SPLITTER = "\\s*:\\s*";

    private static final String COMMA_SPLITTER = "\\s*,\\s*";

    private static final String SENTINEL_MASTERS = "sentinel_masters";

    private int num;

    private List<String> monitors;

    public SentinelMonitors(int num) {
        this.num = num;
        this.monitors = Lists.newArrayListWithExpectedSize(num);
    }

    public void addMonitor(String monitor) {
        this.monitors.add(monitor);
    }

    public static SentinelMonitors parseFromString(String infoSentinel) {

        InfoResultExtractor extractor = new InfoResultExtractor(infoSentinel);
        int monitorNumbers = extractor.extractAsInteger(SENTINEL_MASTERS);

        String key = "master";

        SentinelMonitors sentinelMonitors = new SentinelMonitors(monitorNumbers);

        String[] lines = StringUtil.splitByLineRemoveEmpty(infoSentinel);

        // master0:name=cluster_mengshard1,status=ok,address=10.2.58.242:6399,slaves=1,sentinels=3
        for(String line : lines) {
            line = line.trim();
            if(line.startsWith(key)) {
                String[] keyAndVal = StringUtil.splitRemoveEmpty(COLON_SPLITTER, line);
                String monitorInfo = keyAndVal[1];

                String[] info = StringUtil.splitRemoveEmpty(COMMA_SPLITTER, monitorInfo);
                String monitorName = info[0].substring(5);

                sentinelMonitors.addMonitor(monitorName);
            }
        }
        return sentinelMonitors;
    }

    public List<String> getMonitors() {
        return this.monitors;
    }
}
