package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.SERVER_TYPE;

/**
 * @author wenchao.meng
 *         Mar 17, 2021
 */
public interface MasterStats {

    void increaseDefaultReplicationInputBytes(long bytes);

    void setMasterState(MASTER_STATE masterState);

    void setMasterRole(Endpoint endpoint, SERVER_TYPE serverType);

    SERVER_TYPE lastMasterType();

    long getCommandBPS();

    long getCommandTotalLength();



}
