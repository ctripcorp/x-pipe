package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;

public class SocketErrorException extends RuntimeException {
    public SocketErrorException(IOException cause) {
        super(cause);
    }
}
