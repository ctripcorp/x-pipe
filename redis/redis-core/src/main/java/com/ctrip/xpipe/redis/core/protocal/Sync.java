package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author lishanglin
 * date 2022/2/23
 */
public interface Sync extends Command<Object> /*, Closeable */ {

    String FULL_SYNC = "FULLRESYNC";

    String PARTIAL_SYNC = "CONTINUE";

    String PSYNC = "PSYNC";
    String XSYNC = "XSYNC";

    String SIDNO_SEPARATOR = ",";

    void addSyncObserver(SyncObserver observer);

    void close();

    enum SYNC_STATE {
        SYNC_COMMAND_WAIT_RESPONSE,
        READING_RDB,
        READING_COMMANDS
    }

}
