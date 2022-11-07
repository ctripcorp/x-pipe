package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;

/**
 * @author ayq
 * <p>
 * 2022/11/3 14:29
 */
public interface ApplierMetaService {

    void updateApplierStatus(String dcId, String clusterId, String shardId, ApplierMeta newActiveApplier) throws ResourceNotFoundException;
}
