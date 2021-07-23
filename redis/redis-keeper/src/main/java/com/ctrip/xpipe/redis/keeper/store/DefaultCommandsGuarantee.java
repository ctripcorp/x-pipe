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

    private final long beginOffset;

    private final long offset;

    private final long startAt;

    private final long timeoutAt;

    private static final Logger logger = LoggerFactory.getLogger(DefaultCommandsGuarantee.class);

    public DefaultCommandsGuarantee(CommandsListener commandsListener, long beginOffset, long offset, long timeoutMill) {
        this.commandsListener = commandsListener;
        this.beginOffset = beginOffset;
        this.offset = offset;
        this.startAt = System.currentTimeMillis();
        this.timeoutAt = startAt + timeoutMill;
    }

    @Override
    public long getNeededCommandOffset() {
        return offset - beginOffset;
    }

    @Override
    public boolean isFinish() {
        Long ackOffset = commandsListener.processedOffset();
        if (null != ackOffset && ackOffset > beginOffset) {
            logger.info("[finish][{}] ack {}", this, ackOffset);
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
                ", offset=" + offset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultCommandsGuarantee that = (DefaultCommandsGuarantee) o;
        return offset == that.offset &&
                commandsListener.equals(that.commandsListener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commandsListener, offset);
    }
}
