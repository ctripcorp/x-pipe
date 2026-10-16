package com.ctrip.xpipe.redis.keeper.storage;

// Thrown when a read offset is not in cache and backing FS is never touched.
public class CannotReadPositionInNoFsException extends RuntimeException {

    public CannotReadPositionInNoFsException(String path, long offset) {
        super("cannot read offset " + offset + " of " + path + " when backing FS mode is NO_FS");
    }
}
