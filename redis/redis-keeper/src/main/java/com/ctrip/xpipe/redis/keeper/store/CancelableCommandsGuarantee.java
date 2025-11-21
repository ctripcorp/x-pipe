package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.store.CommandsGuarantee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CancelableCommandsGuarantee implements CommandsGuarantee {

    private boolean canceled = false;

    private final long backlogOffset;

    private final long startAt;

    private final long timeoutAt;

    private static final Logger logger = LoggerFactory.getLogger(CancelableCommandsGuarantee.class);

    public CancelableCommandsGuarantee(long backlogOffset, long startAt, long timeoutMill) {
        this.backlogOffset = backlogOffset;
        this.startAt = System.currentTimeMillis();
        this.timeoutAt = startAt + timeoutMill;
    }

    @Override
    public void cancel() {
        this.canceled = true;
    }

    @Override
    public long getBacklogOffset() {
        return backlogOffset;
    }

    @Override
    public boolean isFinish() {
        return canceled;
    }

    @Override
    public boolean isTimeout() {
        long current = System.currentTimeMillis();
        if (canceled) {
            logger.info("[timeout][canceled] {}", this);
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
}
