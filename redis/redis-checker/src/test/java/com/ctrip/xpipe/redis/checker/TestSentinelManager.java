package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/15
 */
public class TestSentinelManager implements SentinelManager {

    @Override
    public void removeSentinelMonitor(Sentinel sentinel, String sentinelMonitorName) {

    }

    @Override
    public HostPort getMasterOfMonitor(Sentinel sentinel, String sentinelMonitorName) {
        return null;
    }

    @Override
    public String infoSentinel(Sentinel sentinel) {
        return null;
    }

    @Override
    public void monitorMaster(Sentinel sentinel, String sentinelMonitorName, HostPort master, int quorum) {

    }

    @Override
    public List<HostPort> slaves(Sentinel sentinel, String sentinelMonitorName) {
        return null;
    }

    @Override
    public void reset(Sentinel sentinel, String sentinelMonitorName) {

    }

    @Override
    public void sentinelSet(Sentinel sentinel, String sentinelMonitorName, String[] configs) {

    }

    @Override
    public void sentinelConfigSet(Sentinel sentinel, String configName, String configValue) {

    }
}
