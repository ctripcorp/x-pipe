package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 15:40
 */
public interface NetworkCommunication {

    Endpoint endpoint();

    void connect(Endpoint endpoint, Object... states);

    void disconnect();

}
