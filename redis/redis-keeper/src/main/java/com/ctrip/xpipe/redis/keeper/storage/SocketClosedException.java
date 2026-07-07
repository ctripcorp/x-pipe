package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.ClosedChannelException;

public class SocketClosedException extends RuntimeException {
    public SocketClosedException(ClosedChannelException cause) {
        super(cause);
    }
}
