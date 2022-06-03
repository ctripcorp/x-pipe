package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.keeper.applier.lwm.ApplierLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.lwm.DefaultLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.xsync.ApplierXsyncReplication;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultXsyncReplication;

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
    public ApplierXsyncReplication replication;

    @InstanceDependency
    public AsyncRedisClient client;

    @InstanceDependency
    public RedisOpParser parser;

    public DefaultApplierServer(String clusterName, RedisOpParser parser) throws Exception {

        this.sequence = new DefaultSequenceController();
        this.lwmManager = new DefaultLwmManager();
        this.replication = new DefaultXsyncReplication();

        this.client = AsyncRedisClientFactory.DEFAULT.getOrCreateClient(clusterName);
        this.parser = parser;
    }

    @Override
    public SERVER_ROLE role() {
        return SERVER_ROLE.APPLIER;
    }
}
