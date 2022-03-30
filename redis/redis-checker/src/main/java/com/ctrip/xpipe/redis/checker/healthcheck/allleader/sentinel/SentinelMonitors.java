package com.ctrip.xpipe.redis.checker.healthcheck.allleader.sentinel;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class SentinelMonitors {
    private static final String COLON_SPLITTER = "\\s*:\\s*";

    private static final String COMMA_SPLITTER = "\\s*,\\s*";

    private static final String EQUAL_SIGN_SPLITTER = "\\s*=\\s*";

    private static final String SENTINEL_MASTERS = "sentinel_masters";

    private int num;

    private List<String> monitors;

    private List<Pair<String, HostPort>> masters = new ArrayList<>();

    public SentinelMonitors(int num) {
        this.num = num;
        this.monitors = Lists.newArrayListWithExpectedSize(num);
    }

    public void addMonitor(String monitor) {
        this.monitors.add(monitor);
    }

    public void addMaster(Pair<String, HostPort> master){
        this.masters.add(master);
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
                String[] info = StringUtil.splitRemoveEmpty(COMMA_SPLITTER, line);
                String masterNameString = info[0];
                String masterName = StringUtil.splitRemoveEmpty(EQUAL_SIGN_SPLITTER, masterNameString)[1];
                sentinelMonitors.addMonitor(masterName);

                String[] address = StringUtil.splitRemoveEmpty(EQUAL_SIGN_SPLITTER, info[2]);
                String[] masterInstanceInfo = StringUtil.splitRemoveEmpty(COLON_SPLITTER, address[1]);
                Pair<String, HostPort> masterNameAndHostPort = new Pair<>(masterName, new HostPort(masterInstanceInfo[0], Integer.parseInt(masterInstanceInfo[1])));
                sentinelMonitors.addMaster(masterNameAndHostPort);
            }
        }
        return sentinelMonitors;
    }

    public List<String> getMonitors() {
        return this.monitors;
    }

    public List<Pair<String, HostPort>> getMasters() {
        return masters;
    }
}
