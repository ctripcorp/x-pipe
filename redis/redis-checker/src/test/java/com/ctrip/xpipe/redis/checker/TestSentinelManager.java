package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.protocal.pojo.SentinelMasterInstance;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/15
 */
public class TestSentinelManager implements SentinelManager {

    @Override
    public Command<String> removeSentinelMonitor(Sentinel sentinel, String sentinelMonitorName) {
        return null;
    }

    @Override
    public Command<SentinelMasterInstance> getMasterOfMonitor(Sentinel sentinel, String sentinelMonitorName) {
        return null;
    }

    @Override
    public Command<String> infoSentinel(Sentinel sentinel) {
        return null;
    }

    @Override
    public Command<String> monitorMaster(Sentinel sentinel, String sentinelMonitorName, HostPort master, int quorum) {
        return null;
    }

    @Override
    public Command<List<HostPort>> slaves(Sentinel sentinel, String sentinelMonitorName) {
        return null;
    }

    @Override
    public Command<Long> reset(Sentinel sentinel, String sentinelMonitorName) {
        return null;
    }

    @Override
    public Command<String> sentinelSet(Sentinel sentinel, String sentinelMonitorName, String[] configs) {
        return null;
    }

    @Override
    public Command<String> sentinelConfigSet(Sentinel sentinel, String configName, String configValue) {
        return null;
    }
}
