package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.utils.VersionUtils;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 22, 2018
 */
public class XRedisPartialTest extends AbstractKeeperIntegratedSingleDc {

    private static final String versionCheckContinueMin = "1.0.1";

    @Override
    protected String getXpipeMetaConfigFile() {
        return "one_keeper.xml";
    }

    @Test
    public void testContinueWhenKeeperRefusedConnectionWhenAnsweringPsyncCommand() throws Exception {

        RedisMeta slaveMeta = getRedisSlaves().get(0);
        String xRedisVersion = getXRedisVersion(slaveMeta);
        if (!VersionUtils.ge(xRedisVersion, versionCheckContinueMin)) {
            logger.info("do not check xredis version below than {}", versionCheckContinueMin);
            return;
        }


        RedisKeeperServer redisKeeperServer = getRedisKeeperServer(activeKeeper);
        int backlogActiveCount = getBackLogActiveCount(slaveMeta);
        Assert.assertEquals(1, backlogActiveCount);

        Jedis slaveJedis = createJedis(slaveMeta);
        String slaveof = slaveJedis.slaveof("127.0.0.1", randomPort());
        sleep(1100);
        slaveJedis.slaveof("127.0.0.1", activeKeeper.getPort());
        sleep(1100);

        backlogActiveCount = getBackLogActiveCount(slaveMeta);
        logger.info("[getBackLogActiveCount]{}", backlogActiveCount);
        Assert.assertEquals(1, backlogActiveCount);


        //be sure slave can accpet partial sync
        Jedis master = createJedis(redisMaster);
        master.slaveof("127.0.0.1", 0);

        int syncFull1 = Integer.parseInt(getInfoKey(slaveMeta, "stats", "sync_full"));
        int syncPartialOk1 = Integer.parseInt(getInfoKey(slaveMeta, "stats", "sync_partial_ok"));
        logger.info("{}, {}", syncFull1, syncPartialOk1);

        slaveJedis.slaveofNoOne();
        master.slaveof(slaveMeta.getIp(), slaveMeta.getPort());
        sleep(1200);

        int syncFull2 = Integer.parseInt(getInfoKey(slaveMeta, "stats", "sync_full"));
        int syncPartialOk2 = Integer.parseInt(getInfoKey(slaveMeta, "stats", "sync_partial_ok"));
        logger.info("{}, {}", syncFull2, syncPartialOk2);

        Assert.assertEquals(syncFull1, syncFull2);
        Assert.assertEquals(syncPartialOk1 + 1, syncPartialOk2);


    }

    private int getBackLogActiveCount(RedisMeta slaveMeta) throws Exception {
        return Integer.parseInt(getInfoKey(slaveMeta, "replication", "repl_backlog_active"));
    }


    @Override
    protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeperMeta, File baseDir, KeeperConfig keeperConfig, LeaderElectorManager leaderElectorManager, KeepersMonitorManager keeperMonitorManager) {

        return new DefaultRedisKeeperServer(keeperMeta.parent().getDbId(), keeperMeta, keeperConfig, baseDir, leaderElectorManager,
                keeperMonitorManager, resourceManager) {

            private int count = 0;

            @Override
            public RedisKeeperServerState getRedisKeeperServerState() {

                RedisKeeperServerState redisKeeperServerState = super.getRedisKeeperServerState();
                return (RedisKeeperServerState) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{RedisKeeperServerState.class}, new InvocationHandler() {

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        if (method.getName().equals("psync")) {
                            count++;
                            logger.info("[psync][count]{}, {}", args, count);
                            if (count == 2) {
                                RedisClient redisClient = (RedisClient) args[0];
                                logger.info("[psync][close client]{}, {}", count, redisClient);
                                redisClient.close();
                            }

                        }

                        return method.invoke(redisKeeperServerState, args);
                    }
                });
            }
        };
    }

}
