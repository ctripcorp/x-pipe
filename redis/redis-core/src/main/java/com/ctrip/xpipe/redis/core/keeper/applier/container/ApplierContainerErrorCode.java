package com.ctrip.xpipe.redis.core.keeper.applier.container;

/**
 * @author ayq
 * <p>
 * 2022/4/2 16:55
 */
public enum ApplierContainerErrorCode {
    INTERNAL_EXCEPTION,
    APPLIER_ALREADY_EXIST,
    APPLIER_NOT_EXIST,
    APPLIER_ALREADY_STARTED,
    APPLIER_ALREADY_STOPPED,
    APPLIER_ALREADY_DELETED
}
