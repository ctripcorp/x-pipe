package com.ctrip.xpipe.redis.keeper.exception.psync;

import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReason;
import io.netty.channel.Channel;

/**
 * @author Slight
 *
 *         Mar 02, 2021 5:01 PM
 */
public class PsyncMasterDisconnectedException extends PsyncRuntimeException {

    public PsyncMasterDisconnectedException(Channel channel) {
        super("[psync] master close connection:" + channel);
    }

    @Override
    public PsyncFailReason toReason() {
        return PsyncFailReason.MASTER_DISCONNECTED;
    }
}
