package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author lishanglin
 * date 2022/2/23
 */
public interface Xsync extends Command<Object> /*, Closeable */ {

    String FULL_SYNC = "FULLRESYNC";

    String PARTIAL_SYNC = "CONTINUE";

    String SIDNO_SEPARATOR = ",";

    void addXsyncObserver(XsyncObserver observer);

    enum XSYNC_STATE {
        XSYNC_COMMAND_WAIT_RESPONSE,
        READING_RDB,
        READING_COMMANDS
    }

}
