package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.store.CommandsGuarantee;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2021/7/21
 */
public class DefaultCommandsGuarantee implements CommandsGuarantee {

    private final CommandsListener commandsListener;

    private final long backlogOffset;

    private final long startAt;

    private final long timeoutAt;

    private static final Logger logger = LoggerFactory.getLogger(DefaultCommandsGuarantee.class);

    public DefaultCommandsGuarantee(CommandsListener commandsListener, long backlogOffset, long timeoutMill) {
        this.commandsListener = commandsListener;
        this.backlogOffset = backlogOffset;
        this.startAt = System.currentTimeMillis();
        this.timeoutAt = startAt + timeoutMill;
    }

    @Override
    public long getBacklogOffset() {
        return backlogOffset;
    }

    @Override
    public boolean isFinish() {
        Long ackBacklogOffset = commandsListener.processedBacklogOffset();
        if (null != ackBacklogOffset && ackBacklogOffset > backlogOffset) {
            logger.info("[finish][{}] ack backlog offset {}", this, ackBacklogOffset);
            return true;
        }
        return false;
    }

    @Override
    public boolean isTimeout() {
        long current = System.currentTimeMillis();
        if (!commandsListener.isOpen()) {
            logger.info("[timeout][listener close] {}", this);
            return true;
        } else if (current > timeoutAt) {
            logger.info("[timeout][timeout] {}", this);
            return true;
        } else if (current < startAt) {
            logger.info("[timeout][system time rollback] {}", this);
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return "DefaultCommandsGuarantee{" +
                "commandsListener=" + commandsListener +
                ", backlogOffset=" + backlogOffset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultCommandsGuarantee that = (DefaultCommandsGuarantee) o;
        return backlogOffset == that.backlogOffset &&
                commandsListener.equals(that.commandsListener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commandsListener, backlogOffset);
    }
}
