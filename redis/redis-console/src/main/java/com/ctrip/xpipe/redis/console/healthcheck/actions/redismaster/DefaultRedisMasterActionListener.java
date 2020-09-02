package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.*;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class DefaultRedisMasterActionListener implements RedisMasterActionListener, OneWaySupport, BiDirectionSupport {

    private RedisService redisService;

    private MetaCache metaCache;

    private MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper;

    private static final Logger logger = LoggerFactory.getLogger(DefaultRedisMasterActionListener.class);

    private ExecutorService executors;

    @Autowired
    public DefaultRedisMasterActionListener(RedisService redisService, MetaCache metaCache,
                                            MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper) {
        this.redisService = redisService;
        this.metaCache = metaCache;
        this.metaServerConsoleServiceManagerWrapper = metaServerConsoleServiceManagerWrapper;
        executors = Executors.newFixedThreadPool(100, XpipeThreadFactory.create("RedisMasterJudgement"));
    }

    @Override
    public void onAction(RedisMasterActionContext redisMasterActionContext) {
        Server.SERVER_ROLE redisRole = redisMasterActionContext.getResult();
        RedisHealthCheckInstance instance = redisMasterActionContext.instance();

        if(redisRole.equals(Server.SERVER_ROLE.UNKNOWN)) {
            handleUnknownRole(instance);
            return;
        }

        boolean actualMaster = redisRole.equals(Server.SERVER_ROLE.MASTER);
        RedisRoleState state = RedisRoleState.getFrom(instance.getRedisInstanceInfo().isMaster(), actualMaster);
        if(state.shouldBeCorrect()) {
            updateRedisRoleInDB(instance, state);
        }
    }

    protected void handleUnknownRole(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        String dcId = info.getDcId();
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();

        if (!info.isMaster()) {
            logger.info("[handleUnknownRole][{}] redis role unknown, skip", info.getHostPort());
            return;
        }

        List<HostPort> masters = findMasterInDcClusterShard(dcId, clusterId, shardId);
        if (!masters.contains(info.getHostPort())) {
            logger.info("[handleUnknownRole][{}-{}-{}] expected master {} is not equal to masters in meta cache {}, skip",
                    dcId, clusterId, shardId, info.getHostPort(), masters);
            return;
        }
        if (masters.size() <= 1) {
            logger.info("[handleUnknownRole][{}-{}-{}] no confuse on master {}, skip", dcId, clusterId, shardId, info.getHostPort());
            return;
        }

        new FinalMasterJudgeCommand(dcId, clusterId, shardId).execute(executors).addListener(new CommandFutureListener<HostPort>() {
            @Override
            public void operationComplete(CommandFuture<HostPort> commandFuture) throws Exception {
                if (commandFuture.isSuccess()) {
                    HostPort finalMaster = commandFuture.get();

                    if (finalMaster.equals(info.getHostPort())) {
                        logger.info("[handleUnknownRole] meta server consider master this master {}", finalMaster);
                    } else {
                        logger.info("[handleUnknownRole] meta server consider {} as master, not {}", finalMaster, info.getHostPort());
                        updateRedisRoleInDB(instance, RedisRoleState.EXPECT_SLAVE_ACTUAL_MASTER);
                    }
                } else {
                    logger.info("[FinalMasterJudgeCommand][{}-{}-{}] get master from meta server fail", dcId, clusterId, shardId, commandFuture.cause());
                }
            }
        });
    }

    private List<HostPort> findMasterInDcClusterShard(String dcId, String clusterId, String shardId) {
        List<HostPort> masters = new ArrayList<>();

        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return masters;

        DcMeta dcMeta = xpipeMeta.findDc(dcId);
        if (null == dcMeta) return masters;

        ClusterMeta clusterMeta = dcMeta.findCluster(clusterId);
        if (null == clusterMeta) return masters;

        ShardMeta shardMeta = clusterMeta.findShard(shardId);
        if (null == shardMeta) return masters;

        shardMeta.getRedises().forEach(redisMeta -> {
            if (redisMeta.isMaster()) masters.add(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
        });

        return masters;
    }

    protected void updateRedisRoleInDB(RedisHealthCheckInstance instance, RedisRoleState state) {
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        try {
            List<RedisTbl> redises = redisService.findRedisesByDcClusterShard(info.getDcId(), info.getClusterId(), info.getShardId());
            for(RedisTbl redis : redises) {
                if(redis.getRedisIp().equals(info.getHostPort().getHost())
                        && redis.getRedisPort() == info.getHostPort().getPort()) {
                    logger.info("[update redis role][{}] {}", info.getHostPort(), state);
                    redis.setMaster(!info.isMaster());
                    info.isMaster(redis.isMaster());
                    redisService.updateBatchMaster(Lists.newArrayList(redis));
                    break;
                }
            }
        } catch (ResourceNotFoundException e) {
            logger.error("[updateRedisRoleInDB] ", e);
        }

    }

    protected enum RedisRoleState {

        ROLE_MATCHED {
            @Override
            boolean shouldBeCorrect() {
                return false;
            }
        }, EXPECT_MASTER_ACTUAL_SLAVE {
            @Override
            boolean shouldBeCorrect() {
                return true;
            }
        }, EXPECT_SLAVE_ACTUAL_MASTER {
            @Override
            boolean shouldBeCorrect() {
                return true;
            }
        };

        abstract boolean shouldBeCorrect();

        public static RedisRoleState getFrom(boolean expectedMaster, boolean actualMaster) {
            if(expectedMaster == actualMaster) {
                return ROLE_MATCHED;
            } else if(expectedMaster) {
                return EXPECT_MASTER_ACTUAL_SLAVE;
            } else {
                return EXPECT_SLAVE_ACTUAL_MASTER;
            }
        }
    }

    class FinalMasterJudgeCommand extends AbstractCommand<HostPort> {

        private String dcId;
        private String clusterId;
        private String shardId;

        public FinalMasterJudgeCommand(String dcId, String clusterId, String shardId) {
            this.dcId = dcId;
            this.clusterId = clusterId;
            this.shardId = shardId;
        }

        @Override
        protected void doExecute() throws Exception {
            RedisMeta master = metaServerConsoleServiceManagerWrapper.getFastService(dcId).getCurrentMaster(clusterId, shardId);
            future().setSuccess(new HostPort(master.getIp(), master.getPort()));
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }
    }

}
