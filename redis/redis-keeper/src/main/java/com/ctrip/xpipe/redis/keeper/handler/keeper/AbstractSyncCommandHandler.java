package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.core.protocal.error.NoMasterlinkRedisError;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public abstract class AbstractSyncCommandHandler extends AbstractCommandHandler {

    public static final int WAIT_OFFSET_TIME_MILLI = 60 * 1000;

    @Override
    protected void doHandle(final String[] args, final RedisClient<?> redisClient) throws Exception {
        // in non-psync executor
        final RedisKeeperServer redisKeeperServer = (RedisKeeperServer) redisClient.getRedisServer();

        if(redisKeeperServer.rdbDumper() == null && redisKeeperServer.getReplicationStore().isFresh()){
            redisClient.sendMessage(new RedisErrorParser(new NoMasterlinkRedisError("Can't SYNC while replicationstore fresh")).format());
            return;
        }

        if(!redisKeeperServer.getRedisKeeperServerState().psync(redisClient, args)){
            return;
        }

        final RedisSlave redisSlave  = becomeSlave(redisClient);
        if(redisSlave == null){
            logger.warn("[doHandle][client already slave] {}", redisClient);
            try {
                redisClient.close();
            } catch (IOException e) {
                logger.error("[doHandle]" + redisClient, e);
            }
            return;
        }

        // transfer to psync executor, which will do psync dedicatedly
        redisSlave.processPsyncSequentially(new Runnable() {

            @Override
            public void run() {
                try{
                    innerDoHandle(args, redisSlave, redisKeeperServer);
                }catch(Throwable th){
                    try {
                        logger.error("[run]" + redisClient, th);
                        if(redisSlave.isOpen()){
                            redisSlave.close();
                        }
                    } catch (IOException e) {
                        logger.error("[run][close]" + redisSlave, th);
                    }
                }
            }
        });
    }

    protected abstract RedisSlave becomeSlave(RedisClient<?> redisClient);

    protected abstract void innerDoHandle(final String[] args, final RedisSlave redisSlave, RedisKeeperServer redisKeeperServer) throws IOException;

    protected void doFullSync(RedisSlave redisSlave) {

        try {
            if(logger.isInfoEnabled()){
                logger.info("[doFullSync]" + redisSlave);
            }

            redisSlave.markPsyncProcessed();
            RedisKeeperServer redisKeeperServer = (RedisKeeperServer)redisSlave.getRedisServer();

            //alert full sync
            String alert = String.format("FULL(M)<-%s[%s]", redisSlave.metaInfo(), redisKeeperServer.getReplId());
            EventMonitor.DEFAULT.logAlertEvent(alert);

            redisKeeperServer.fullSyncToSlave(redisSlave);
            redisKeeperServer.getKeeperMonitor().getKeeperStats().increaseFullSync();
        } catch (IOException e) {
            logger.error("[doFullSync][close client]" + redisSlave, e);
            try {
                redisSlave.close();
            } catch (IOException e1) {
                logger.error("[doFullSync]" + redisSlave, e1);
            }
        }
    }

    @Override
    public boolean support(RedisServer server) {
        return server instanceof RedisKeeperServer;
    }
}

