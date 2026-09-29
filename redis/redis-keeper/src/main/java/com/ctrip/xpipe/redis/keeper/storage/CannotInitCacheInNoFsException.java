package com.ctrip.xpipe.redis.keeper.storage;

// Thrown when cache would need to be initialized from backing FS, but FS is never touched.
public class CannotInitCacheInNoFsException extends RuntimeException {

    public CannotInitCacheInNoFsException(String path) {
        super("cannot initialize cache for " + path + " when backing FS mode is NO_FS");
    }
}
