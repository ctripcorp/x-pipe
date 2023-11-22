package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.ApplierInstanceMeta;
import com.ctrip.xpipe.redis.keeper.RedisServer;

/**
 * @author Slight
 * <p>
 * Jun 10, 2022 11:38
 */
public interface ApplierServer extends Lifecycle, RedisServer {

    enum STATE { NONE, ACTIVE, BACKUP }

    int getListeningPort();

    ApplierInstanceMeta getApplierInstanceMeta();

    void setStateActive(Endpoint endpoint, GtidSet gtidSet);

    void setStateBackup();

    void freezeConfig();

    void stopFreezeConfig();

    long getFreezeLastMillis();

    STATE getState();

    Endpoint getUpstreamEndpoint();

    long getEndOffset();

}
