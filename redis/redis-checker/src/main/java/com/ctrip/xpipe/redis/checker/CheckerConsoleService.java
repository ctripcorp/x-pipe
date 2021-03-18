package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import com.ctrip.xpipe.redis.checker.model.HealthCheckResult;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/16
 */
public interface CheckerConsoleService {

    XpipeMeta getXpipeMeta(String console, int clusterPartIndex);

    List<ProxyTunnelInfo> getProxyTunnelInfos(String console);

    void ack(String console, CheckerStatus checkerStatus);

    void report(String console, HealthCheckResult result);

}
