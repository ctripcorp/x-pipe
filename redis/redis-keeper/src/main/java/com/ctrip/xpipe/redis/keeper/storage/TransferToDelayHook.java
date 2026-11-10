package com.ctrip.xpipe.redis.keeper.storage;

/** Creates best-effort transfer timing contexts for one slave connection. */
public interface TransferToDelayHook {

    TransferToDelayHook NOOP = (position, offset) -> TransferToDelayContext.NONE;

    TransferToDelayContext begin(long position, long offset);

    static TransferToDelayContext safeBegin(TransferToDelayHook hook, long position, long offset) {
        if (hook == null) {
            return TransferToDelayContext.NONE;
        }
        try {
            TransferToDelayContext context = hook.begin(position, offset);
            return context == null ? TransferToDelayContext.NONE : context;
        } catch (Throwable t) {
            TransferToDelayMonitorSupport.logFailure("begin", t);
            return TransferToDelayContext.NONE;
        }
    }
}
