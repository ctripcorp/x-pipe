package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionListener;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.tuple.Pair;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface CrossMasterDelayService extends DelayActionListener {

    UnhealthyInfoModel getCurrentDcUnhealthyMasters();

    Map<String, Pair<HostPort, Long>> getPeerMasterDelayFromCurrentDc(String clusterId, String shardId);

    Map<String, Pair<HostPort, Long>> getPeerMasterDelayFromSourceDc(String sourceDcId, String clusterId, String shardId);

}
