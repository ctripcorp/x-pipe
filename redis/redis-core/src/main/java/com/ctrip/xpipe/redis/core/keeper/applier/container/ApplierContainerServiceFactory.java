package com.ctrip.xpipe.redis.core.keeper.applier.container;

import com.ctrip.xpipe.redis.core.entity.ApplierContainerMeta;

/**
 * @author ayq
 * <p>
 * 2022/4/2 12:27
 */
public interface ApplierContainerServiceFactory {
    ApplierContainerService getOrCreateApplierContainerService(ApplierContainerMeta applierContainerMeta);
}
