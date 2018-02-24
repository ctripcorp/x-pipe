package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.core.IVisitor;
import com.ctrip.xpipe.redis.core.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.Executor;

import static com.ctrip.xpipe.redis.console.health.delay.DefaultDelayMonitor.CHECK_CHANNEL;

/**
 * @author chen.zhu
 * <p>
 * Nov 19, 2017
 */
@Component
public class HealthCheckVisitor implements IVisitor {

    @Autowired
    RedisSessionManager sessionManager;

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Resource(name= ConsoleContextConfig.GLOBAL_EXECUTOR)
    private Executor executor;

    @Override
    public void visitCluster(ClusterMeta cluster) {
        for(ShardMeta shard : cluster.getShards().values()) {
            shard.accept(this);
        }
    }

    @Override
    public void visitDc(DcMeta dc) {
        for(ClusterMeta cluster : dc.getClusters().values()) {
            cluster.accept(this);
        }
    }

    @Override
    public void visitKeeper(KeeperMeta keeper) {
        throw new UnsupportedOperationException("no warm up on keeper");
    }

    @Override
    public void visitKeeperContainer(KeeperContainerMeta keeperContainer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visitMetaServer(MetaServerMeta metaServer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visitRedis(RedisMeta redis) {
        RedisSession session = sessionManager.findOrCreateSession(redis.getIp(), redis.getPort());
        this.executor.execute(new Runnable() {
            @Override
            public void run() {
                session.ping(new PingCallback() {
                    @Override
                    public void pong(String pongMsg) {
                        //ignore
                    }

                    @Override
                    public void fail(Throwable th) {
                        logger.debug("[visitRedis] fail {}", th);
                    }
                });
                session.subscribeIfAbsent(CHECK_CHANNEL, new RedisSession.SubscribeCallback() {
                    @Override
                    public void message(String channel, String message) {
                        // ignore
                    }

                    @Override
                    public void fail(Throwable e) {
                        logger.debug("[visitedRedis] subscribe health check channel fail, {}", e);
                    }
                });
            }
        });
    }

    @Override
    public void visitSentinel(SentinelMeta sentinel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visitShard(ShardMeta shard) {
        for(RedisMeta redisMeta : shard.getRedises()) {
            redisMeta.accept(this);
        }
    }

    @Override
    public void visitXpipe(XpipeMeta xpipe) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visitZkServer(ZkServerMeta zkServer) {
        throw new UnsupportedOperationException();
    }
}
