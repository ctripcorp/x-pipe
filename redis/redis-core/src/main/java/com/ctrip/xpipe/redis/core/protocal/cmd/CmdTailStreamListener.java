package com.ctrip.xpipe.redis.core.protocal.cmd;

import io.netty.buffer.ByteBuf;

/**
 * Incremental cmd bytes from {@link CmdTailGapAllowedSync}.
 * The implementation may hold {@code buf} only for the duration of this callback;
 * the caller does not retain it.
 */
public interface CmdTailStreamListener {

    void onCommands(ByteBuf buf);
}
