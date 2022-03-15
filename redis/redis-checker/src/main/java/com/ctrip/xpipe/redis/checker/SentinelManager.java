package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */
public interface SentinelManager {

    Command<String> removeSentinelMonitor(Sentinel sentinel, String sentinelMonitorName);

    Command<HostPort> getMasterOfMonitor(Sentinel sentinel, String sentinelMonitorName);

    Command<String> infoSentinel(Sentinel sentinel);

    Command<String> monitorMaster(Sentinel sentinel, String sentinelMonitorName, HostPort master, int quorum);

    Command<List<HostPort>> slaves(Sentinel sentinel, String sentinelMonitorName);

    Command<Long> reset(Sentinel sentinel, String sentinelMonitorName);

    Command<String> sentinelSet(Sentinel sentinel, String sentinelMonitorName, String[] configs);

    Command<String> sentinelConfigSet(Sentinel sentinel, String configName, String configValue);

}
