package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingAction;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionContext;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class AbstractHealthCheckActionTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private List<HealthCheckActionListener> listeners;

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @Test
    public void testOnActionWithPing() {
        DcMeta dcMeta = new DcMeta("dc");
        ClusterMeta clusterMeta = new ClusterMeta("cluster");
        ShardMeta shardMeta = new ShardMeta("shard");
        RedisMeta redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setMaster(null);
        shardMeta.addRedis(redisMeta);
        clusterMeta.addShard(shardMeta);
        dcMeta.addCluster(clusterMeta);
        RedisHealthCheckInstance instance = instanceManager.getOrCreate(redisMeta);
        PingAction action = new PingAction(scheduled, instance, executors);
        action.addListeners(listeners);
        action.notifyListeners(new PingActionContext(instance, true));
        instanceManager.remove(new HostPort(instance.getEndpoint().getHost(), instance.getEndpoint().getPort()));
    }
}