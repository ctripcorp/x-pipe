package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;

public class EIOException extends RuntimeException {

    /**
     * TFS reports Linux EIO when the current FD is no longer usable. The FD keeps returning EIO
     * until it is reopened; after reopen the file size must be re-read before writes resume.
     */
    public EIOException(IOException cause) {
        super(cause);
    }
}
