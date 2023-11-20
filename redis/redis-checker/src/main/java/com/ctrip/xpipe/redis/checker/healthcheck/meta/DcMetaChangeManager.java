package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface DcMetaChangeManager extends Startable, Stoppable {

    void compare(DcMeta future, DcMeta allFutureDcMeta);

}
