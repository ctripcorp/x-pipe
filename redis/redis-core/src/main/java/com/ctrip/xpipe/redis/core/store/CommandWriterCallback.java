package com.ctrip.xpipe.redis.core.store;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * @author lishanglin
 *
 * Callback interface for command writing operations.
 * This interface is used to decouple IndexStore from CommandStore,
 * allowing IndexStore to write commands through this callback without
 * directly depending on CommandStore implementation.
 */
public interface CommandWriterCallback {
    
    /**
     * Write parsed command to command store
     * @param commandBuf command data
     * @return number of bytes written
     * @throws IOException if write operation fails
     */
    int writeCommand(ByteBuf commandBuf) throws IOException;
    
    /**
     * Get current write offset
     * @return current offset
     */
    long getCurrentOffset();

    long getCmdFileLen();
    
    /**
     * Get CommandWriter for file rotation and other operations.
     * Note: This method is also defined in CommandStore interface,
     * so classes implementing both interfaces only need to implement once.
     * @return CommandWriter instance
     */
    CommandWriter getCommandWriter();
}
