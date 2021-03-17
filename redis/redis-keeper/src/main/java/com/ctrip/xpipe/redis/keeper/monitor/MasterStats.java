package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;

/**
 * @author wenchao.meng
 *         Mar 17, 2021
 */
public interface MasterStats {

    void increaseDefaultReplicationInputBytes(long bytes);

    void setMasterState(MASTER_STATE masterState);

    long getCommandBPS();

    long getCommandTotalLength();

}
