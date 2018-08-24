package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface DcMetaChangeManager {

    void compare(DcMeta future);

}
