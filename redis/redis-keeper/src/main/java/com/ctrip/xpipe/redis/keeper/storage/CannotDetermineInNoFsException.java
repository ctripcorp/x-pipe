package com.ctrip.xpipe.redis.keeper.storage;

// Thrown when a filesystem fact cannot be determined because backing FS is never touched.
public class CannotDetermineInNoFsException extends RuntimeException {

    public CannotDetermineInNoFsException(String operation) {
        super("cannot determine " + operation + " when backing FS mode is NO_FS");
    }
}
