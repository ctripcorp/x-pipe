package com.ctrip.xpipe.redis.meta.server.keeper.applier.elect;

import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierActiveElectAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ayq
 * <p>
 * 2022/4/11 22:11
 */
public abstract class AbstractApplierActiveElectAlgorithm implements ApplierActiveElectAlgorithm {
    protected Logger logger = LoggerFactory.getLogger(getClass());
}
