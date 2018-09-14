package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface MetaChangeManager extends Startable, Stoppable {

    DcMetaChangeManager getOrCreate(String dcId);

    void ignore(String dcId);

    void startIfPossible(String dcId);
}
