package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.SERVER_TYPE;
import com.ctrip.xpipe.redis.keeper.monitor.MasterStats;
import com.ctrip.xpipe.utils.ObjectUtils;
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

    private SERVER_TYPE lastMasterType;
    private Endpoint lastMasterEndPoint;
    private SERVER_TYPE currentMasterType;
    private Endpoint currentMasterEndPoint;

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
    public void setMasterRole(Endpoint endpoint, SERVER_TYPE serverType) {

        if (!ObjectUtils.equals(endpoint, currentMasterEndPoint)) {
            logger.info("[setMasterRole][endpoint change]{}({})->{}({})", this.currentMasterEndPoint, this.currentMasterType, endpoint, serverType);
            this.lastMasterEndPoint = this.currentMasterEndPoint;
            this.lastMasterType = this.currentMasterType;

            this.currentMasterEndPoint = endpoint;
            this.currentMasterType = serverType;
        } else {
            if (serverType != currentMasterType) {
                logger.info("[setMasterRole][endpoint role change]{}({})->({})", this.currentMasterEndPoint, this.currentMasterType, serverType);
                this.currentMasterType = serverType;
            }
        }
    }

    @Override
    public SERVER_TYPE lastMasterType() {
        return lastMasterType == null ? SERVER_TYPE.UNKNOWN : lastMasterType;
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
