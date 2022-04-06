package com.ctrip.xpipe.redis.meta.server.keeper.applier.manager;

import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.meta.server.exception.MetaServerException;

/**
 * @author ayq
 * <p>
 * 2022/4/6 16:29
 */
public class DeleteApplierStillAliveException extends MetaServerException {

    private static final long serialVersionUID = 1L;

    public DeleteApplierStillAliveException(ApplierMeta currentApplier){
        super(String.format("current keeper still alive:%s:%d", currentApplier.getIp(), currentApplier.getPort()));
        setOnlyLogMessage(true);
    }
}
