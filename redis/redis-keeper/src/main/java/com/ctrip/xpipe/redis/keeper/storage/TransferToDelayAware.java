package com.ctrip.xpipe.redis.keeper.storage;

/** A transfer object that can receive the monitor hook of its destination slave. */
public interface TransferToDelayAware {

    void bindTransferToDelayHook(TransferToDelayHook hook);
}
