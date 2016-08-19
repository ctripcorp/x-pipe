package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@SuppressWarnings("serial")
public class KeeperContainerException extends RedisRuntimeException {

    public KeeperContainerException(String message) {
        super(message);
    }

    public KeeperContainerException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> KeeperContainerException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
