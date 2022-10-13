package com.ctrip.xpipe.redis.meta.server.keeper.applier.elect;

import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author ayq
 * <p>
 * 2022/4/11 22:52
 */
@Component
public class DefaultApplierActiveElectAlgorithmManager implements ApplierActiveElectAlgorithmManager {

    private static Logger logger = LoggerFactory.getLogger(DefaultApplierActiveElectAlgorithmManager.class);

    @SuppressWarnings("unused")
    @Autowired
    private DcMetaCache dcMetaCache;

    @Override
    public ApplierActiveElectAlgorithm get(Long clusterDbId, Long shardDbId) {

        logger.debug("[get][active dc, use default]");
        return new DefaultApplierActiveElectAlgorithm();
    }

    public void setDcMetaCache(DcMetaCache dcMetaCache) {
        this.dcMetaCache = dcMetaCache;
    }
}
