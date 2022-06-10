package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.keeper.applier.lwm.ApplierLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.lwm.DefaultLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.xsync.ApplierCommandDispatcher;
import com.ctrip.xpipe.redis.keeper.applier.xsync.ApplierXsyncReplication;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultCommandDispatcher;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultXsyncReplication;
import com.ctrip.xpipe.service.client.redis.CRedisAsyncClientFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 08:16
 */
public class DefaultApplierServer extends AbstractInstanceNode implements ApplierServer {

    /* component */

    @InstanceDependency
    public ApplierSequenceController sequence;

    @InstanceDependency
    public ApplierLwmManager lwmManager;

    @InstanceDependency
    public ApplierXsyncReplication replication;

    @InstanceDependency
    public ApplierCommandDispatcher dispatcher;

    /* cardinal info */

    @InstanceDependency
    public AsyncRedisClient client;

    @InstanceDependency
    //TODO: distinguish gtid_received, gtid_executed, gtid_in_request
    public AtomicReference<GtidSet> gtidSet;

    /* utility */

    @InstanceDependency
    public XpipeNettyClientKeyedObjectPool pool;

    @InstanceDependency
    public RedisOpParser parser;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    public DefaultApplierServer(String clusterName,
                                XpipeNettyClientKeyedObjectPool pool,
                                RedisOpParser parser,
                                ScheduledExecutorService scheduled) throws Exception {

        this.sequence = new DefaultSequenceController();
        this.lwmManager = new DefaultLwmManager(); //TODO: init lwm with gtidSet
        this.replication = new DefaultXsyncReplication();
        this.dispatcher = new DefaultCommandDispatcher();

        this.client = new CRedisAsyncClientFactory().getOrCreateClient(clusterName);
//        this.client = AsyncRedisClientFactory.DEFAULT.getOrCreateClient(clusterName);
        this.gtidSet = new AtomicReference<>();

        this.pool = pool;
        this.parser = parser;
        this.scheduled = scheduled;
    }

    @Override
    public void setState(Endpoint endpoint, GtidSet gtidSet) {
        replication.connect(endpoint, gtidSet);
    }

    @Override
    public SERVER_ROLE role() {
        return SERVER_ROLE.APPLIER;
    }
}
