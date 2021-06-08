package com.ctrip.xpipe.redis.console.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import org.aspectj.weaver.Checker;

import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/16
 */
public interface CheckerManager {

    void refreshCheckerStatus(CheckerStatus checkerStatus);

    List<Map<HostPort, CheckerStatus>> getCheckers();

    List<CheckerService> getLeaderCheckerServices();
}
