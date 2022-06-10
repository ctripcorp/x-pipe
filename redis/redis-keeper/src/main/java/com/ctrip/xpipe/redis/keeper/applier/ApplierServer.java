package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.ApplierInstanceMeta;

/**
 * @author Slight
 * <p>
 * Jun 10, 2022 11:38
 */
public interface ApplierServer extends Lifecycle {

    int getListeningPort();

    ApplierInstanceMeta getApplierInstanceMeta();

    void setState(Endpoint endpoint, GtidSet gtidSet);

}
