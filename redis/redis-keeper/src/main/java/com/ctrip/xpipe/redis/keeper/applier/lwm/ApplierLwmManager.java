package com.ctrip.xpipe.redis.keeper.applier.lwm;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author Slight
 * <p>
 * May 30, 2022 01:42
 */
public interface ApplierLwmManager extends Lifecycle {

     void submit(String gtid);
}
