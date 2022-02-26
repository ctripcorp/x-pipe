package com.ctrip.xpipe.redis.core.protocal;

/**
 * @author lishanglin
 * date 2022/2/23
 */
public interface Xsync {

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
