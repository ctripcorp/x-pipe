package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.crossdc.AbstractCDLAHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 08, 2018
 */
public class RedisMasterCheckAction extends AbstractCDLAHealthCheckAction {

    private static final Logger logger = LoggerFactory.getLogger(RedisMasterCheckAction.class);

    private RedisService redisService;

    private Server.SERVER_ROLE serverRole;

    public RedisMasterCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                                  ExecutorService executors, RedisService service) {
        super(scheduled, instance, executors);
        this.redisService = service;
    }

    @Override
    protected void doTask() {
        RedisInstanceInfo info = getActionInstance().getRedisInstanceInfo();
        if(!info.isInActiveDc()) {
            logger.debug("[doTask] not in backup dc: {}", info);
            return;
        }
        CommandFuture<Role> future = getActionInstance().getRedisSession().role(new RedisSession.RollCallback() {
            @Override
            public void role(String role) {
                serverRole = Server.SERVER_ROLE.of(role);
            }

            @Override
            public void fail(Throwable e) {
                serverRole = Server.SERVER_ROLE.UNKNOWN;
            }
        });
        future.addListener(new CommandFutureListener<Role>() {
            @Override
            public void operationComplete(CommandFuture<Role> commandFuture) throws Exception {
                new AsyncRun() {
                    @Override
                    protected void doRun0() {
                        checkMaster();
                    }
                }.run();
            }
        });
    }

    private void checkMaster() {
        boolean actualMaster = serverRole.equals(Server.SERVER_ROLE.MASTER);
        RedisRoleState state = RedisRoleState.getFrom(getActionInstance().getRedisInstanceInfo().isMaster(), actualMaster);
        if(state.shouldBeCorrect()) {
            updateRedisRoleInDB(state);
        }
    }

    private void updateRedisRoleInDB(RedisRoleState state) {
        RedisInstanceInfo info = getActionInstance().getRedisInstanceInfo();
        try {
            List<RedisTbl> redises = redisService.findRedisesByDcClusterShard(info.getDcId(), info.getClusterId(), info.getShardId());
            for(RedisTbl redis : redises) {
                if(redis.getRedisIp().equals(info.getHostPort().getHost())
                        && redis.getRedisPort() == info.getHostPort().getPort()) {
                    logger.info("[update redis role][{}] {}", info.getHostPort(), state.name());
                    redis.setMaster(!info.isMaster());
                    info.isMaster(redis.isMaster());
                    redisService.updateBatchMaster(Lists.newArrayList(redis));
                }
            }
        } catch (ResourceNotFoundException e) {
            logger.error("[updateRedisRoleInDB] ", e);
        }

    }

    private enum RedisRoleState {

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
}
