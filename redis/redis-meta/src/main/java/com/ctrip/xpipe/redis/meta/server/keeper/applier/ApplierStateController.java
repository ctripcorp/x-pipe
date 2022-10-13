package com.ctrip.xpipe.redis.meta.server.keeper.applier;

import com.ctrip.xpipe.redis.core.entity.ApplierTransMeta;

/**
 * @author ayq
 * <p>
 * 2022/4/1 23:29
 */
public interface ApplierStateController {

    void addApplier(ApplierTransMeta applierTransMeta);

    void removeApplier(ApplierTransMeta applierTransMeta);
}
