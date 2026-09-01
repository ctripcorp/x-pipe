package com.ctrip.xpipe.redis.keeper.storage;

// Thrown when a write would bypass cache while backing FS is never touched,
// or when a hole created under NO_FS cache eviction makes uncached write impossible.
public class CannotWriteWithoutCacheInNoFsException extends RuntimeException {

    public CannotWriteWithoutCacheInNoFsException(String path, String reason) {
        super("cannot write without cache for " + path + " when backing FS mode is NO_FS: " + reason);
    }
}
