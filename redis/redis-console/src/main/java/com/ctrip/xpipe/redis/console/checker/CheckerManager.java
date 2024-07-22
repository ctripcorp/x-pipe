package com.ctrip.xpipe.redis.console.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;

import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/16
 */
public interface CheckerManager {

    void refreshCheckerStatus(CheckerStatus checkerStatus);

    List<Map<HostPort, CheckerStatus>> getCheckers();

    List<ConsoleCheckerService> getLeaderCheckerServices();

    List<HostPort> getClusterCheckerManager(long clusterId);

    HostPort getClusterCheckerLeader(long clusterId);

}
