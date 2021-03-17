package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.monitor.MasterStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 * Mar 17, 2021
 */
public class DefaultMasterStats implements MasterStats {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private long commandsLength;
    private long durationMilli;
    private MASTER_STATE masterState;

    private long lastCommandStartTime;

    @Override
    public void increaseDefaultReplicationInputBytes(long bytes) {
        logger.debug("[increaseDefaultReplicationInputBytes]{}, {}", bytes, masterState);
        if (masterState == MASTER_STATE.REDIS_REPL_CONNECTED) {
            commandsLength += bytes;
        }
    }

    @Override
    public void setMasterState(MASTER_STATE masterState) {
        logger.debug("[setMasterState]{}", masterState);
        if (this.masterState != masterState) {
            if (masterState == MASTER_STATE.REDIS_REPL_CONNECTED) {
                logger.info("[setMasterState][command begin]");
                lastCommandStartTime = System.currentTimeMillis();
            } else if (this.masterState == MASTER_STATE.REDIS_REPL_CONNECTED) {
                durationMilli += System.currentTimeMillis() - lastCommandStartTime;
                logger.info("[setMasterState][command end]duration:{}", durationMilli);
            }
        }
        this.masterState = masterState;
        logger.debug("[setMasterState]{}, {}", this.masterState, masterState);

    }

    @Override
    public long getCommandBPS() {

        long durationMilli = getDurationMilli();
        long durationSeconds = durationMilli / 1000;
        if (durationSeconds == 0) {
            return 0;
        }
        return commandsLength / durationSeconds;
    }

    @Override
    public long getCommandTotalLength() {
        return commandsLength;
    }

    private long getDurationMilli() {

        if (this.masterState == MASTER_STATE.REDIS_REPL_CONNECTED) {
            return durationMilli + (System.currentTimeMillis() - lastCommandStartTime);
        }
        return durationMilli;
    }

    public long getCommandBPMilli() {

        long durationMilli = getDurationMilli();
        if (durationMilli == 0) {
            return 0;
        }
        return commandsLength / durationMilli;
    }

    public long getCommandsLength() {
        return commandsLength;
    }
}
