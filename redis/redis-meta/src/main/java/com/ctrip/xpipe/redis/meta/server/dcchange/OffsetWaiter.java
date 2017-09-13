package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 13, 2017
 */
public interface OffsetWaiter {

    boolean tryWaitfor(HostPort hostPort, MasterInfo masterInfo, ExecutionLog executionLog);

}
