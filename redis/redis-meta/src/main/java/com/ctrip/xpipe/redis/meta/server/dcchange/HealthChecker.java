package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.exception.SimpleErrorMessage;

/**
 * @author wenchao.meng
 *
 * Nov 30, 2016
 */
public interface HealthChecker {

	SimpleErrorMessage check();

}
