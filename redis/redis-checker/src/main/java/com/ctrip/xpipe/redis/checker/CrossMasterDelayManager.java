package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.tuple.Pair;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface CrossMasterDelayManager {

    Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> getAllCrossMasterDelays();

    void updateCrossMasterDelays(Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> delays);

}
