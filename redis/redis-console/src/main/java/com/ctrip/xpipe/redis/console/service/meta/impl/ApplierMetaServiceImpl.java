package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.model.ApplierTbl;
import com.ctrip.xpipe.redis.console.service.ApplierService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.console.service.meta.AbstractMetaService;
import com.ctrip.xpipe.redis.console.service.meta.ApplierMetaService;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * @author ayq
 * <p>
 * 2022/11/3 14:30
 */
@Service
public class ApplierMetaServiceImpl extends AbstractMetaService implements ApplierMetaService {

    @Autowired
    private ApplierService applierService;

    @Override
    public void updateApplierStatus(String dcId, String clusterId, String shardId, ApplierMeta newActiveApplier) throws ResourceNotFoundException {

        List<ApplierTbl> appliers = applierService.findAppliersByDcAndShard(dcId, clusterId, shardId);
        if (CollectionUtils.isEmpty(appliers)) {
            return;
        }

        for (ApplierTbl applier : appliers) {
            if (applier.getContainerId() == newActiveApplier.getApplierContainerId()) {
                applier.setActive(true);
            } else {
                applier.setActive(false);
            }
        }
        applierService.updateBatchApplierActive(appliers);
    }
}
