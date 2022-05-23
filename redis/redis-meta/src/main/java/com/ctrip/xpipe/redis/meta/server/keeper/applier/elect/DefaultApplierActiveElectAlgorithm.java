package com.ctrip.xpipe.redis.meta.server.keeper.applier.elect;

import com.ctrip.xpipe.redis.core.entity.ApplierMeta;

import java.util.List;

/**
 * @author ayq
 * <p>
 * 2022/4/11 22:13
 */
public class DefaultApplierActiveElectAlgorithm extends AbstractApplierActiveElectAlgorithm {

    @Override
    public ApplierMeta select(Long clusterDbId, Long shardDbId, List<ApplierMeta> toBeSelected){

        if(toBeSelected.size() > 0){
            ApplierMeta result = toBeSelected.get(0);
            result.setActive(true);
            return result;
        }
        return null;
    }
}
