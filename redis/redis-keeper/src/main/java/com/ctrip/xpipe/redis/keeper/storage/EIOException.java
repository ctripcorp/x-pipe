package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;

public class EIOException extends RuntimeException {

    public EIOException(IOException cause) {
        super(cause);
    }
}
