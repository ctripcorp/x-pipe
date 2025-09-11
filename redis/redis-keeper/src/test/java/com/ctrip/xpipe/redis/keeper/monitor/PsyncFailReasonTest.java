package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.redis.keeper.exception.psync.*;
import io.netty.channel.Channel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * @author Slight
 * <p>
 * Mar 08, 2021 6:25 PM
 */
public class PsyncFailReasonTest {

    @Test
    public void reason() {
        assertEquals(PsyncFailReason.MASTER_RDB_OFFSET_NOT_CONTINUOUS, new PsyncMasterRdbOffsetNotContinuousRuntimeException(10, 20).toReason());
        assertEquals(PsyncFailReason.CONNECT_MASTER_FAIL, new PsyncConnectMasterFailException(new Exception()).toReason());
        assertEquals(PsyncFailReason.PSYNC_COMMAND_FAIL, new PsyncCommandFailException(new Exception()).toReason());
        assertEquals(PsyncFailReason.MASTER_DISCONNECTED, new PsyncMasterDisconnectedException(mock(Channel.class)).toReason());
        assertEquals(PsyncFailReason.OTHER, new PsyncRuntimeException("Other").toReason());
    }
}