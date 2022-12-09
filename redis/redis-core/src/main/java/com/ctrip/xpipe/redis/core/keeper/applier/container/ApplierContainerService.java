package com.ctrip.xpipe.redis.core.keeper.applier.container;


import com.ctrip.xpipe.redis.core.entity.ApplierInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierTransMeta;

import java.util.List;

/**
 * @author ayq
 * <p>
 * 2022/4/2 12:10
 */
public interface ApplierContainerService {

    void addApplier(ApplierTransMeta applierTransMeta);

    void addOrStartApplier(ApplierTransMeta applierTransMeta);

    void removeApplier(ApplierTransMeta applierTransMeta);

    void startApplier(ApplierTransMeta applierTransMeta);

    void stopApplier(ApplierTransMeta applierTransMeta);

    List<ApplierInstanceMeta> getAllAppliers();
}
