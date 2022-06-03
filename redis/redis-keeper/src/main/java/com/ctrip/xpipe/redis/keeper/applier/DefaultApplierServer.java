package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 08:16
 */
public class DefaultApplierServer extends AbstractInstanceNode {

    @InstanceDependency
    public ApplierSequenceController sequence;

    @InstanceDependency
    public ApplierLwmManager lwmManager;

    @InstanceDependency
    public AsyncRedisClient client;

    public DefaultApplierServer(String clusterName) throws Exception {
        this.sequence = new DefaultSequenceController();
        this.lwmManager = new DefaultLwmManager();
        this.client = AsyncRedisClientFactory.DEFAULT.getOrCreateClient(clusterName);
    }

    @Override
    public SERVER_ROLE role() {
        return SERVER_ROLE.APPLIER;
    }
}
