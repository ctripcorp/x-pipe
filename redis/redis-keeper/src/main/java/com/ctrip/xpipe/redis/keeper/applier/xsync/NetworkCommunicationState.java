package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 15:40
 */
public interface NetworkCommunicationState {

    Endpoint endpoint();
    boolean isConnected();

    void initState(Endpoint endpoint, Object... states);
}
