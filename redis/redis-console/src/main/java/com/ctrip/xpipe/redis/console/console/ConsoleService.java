package com.ctrip.xpipe.redis.console.console;

import com.ctrip.xpipe.redis.console.healthcheck.action.HEALTH_STATE;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
public interface ConsoleService {

    HEALTH_STATE getInstanceStatus(String ip, int port);

    Boolean getInstancePingStatus(String ip, int port);

}
