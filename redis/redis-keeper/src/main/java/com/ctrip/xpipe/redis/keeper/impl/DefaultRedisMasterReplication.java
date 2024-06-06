package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.*;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wenchao.meng
 * <p>
 * Aug 24, 2016
 */
public class DefaultRedisMasterReplication extends AbstractRedisMasterReplication {

    private volatile PARTIAL_STATE partialState = PARTIAL_STATE.UNKNOWN;

    private ScheduledFuture<?> replConfFuture;

    private AtomicReference<Psync> currentPsync = new AtomicReference<>();

    public DefaultRedisMasterReplication(RedisMaster redisMaster, RedisKeeperServer redisKeeperServer,
                                         NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled,
                                         KeeperResourceManager resourceManager) {
        super(redisKeeperServer, redisMaster, nioEventLoopGroup, scheduled, resourceManager);
    }

    @Override
    public boolean tryRordb() {
        // capa rordb as default
        // if the master support rordb, the slaves will most likely support it too
        return true;
    }

    @Override
    protected void doConnect(Bootstrap b) {

        redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTING);

        tryConnect(b).addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {

                if (!future.isSuccess()) {
                    logger.error("[operationComplete][fail connect with master]" + redisMaster, future.cause());
                    scheduleReconnect(masterConnectRetryDelaySeconds * 1000);
                }
            }
        });
    }


    @Override
    public void masterConnected(Channel channel) {

        redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_HANDSHAKE);
        super.masterConnected(channel);
        cancelReplConf();
    }

    @Override
    protected void onReceiveMessage(int messageLength) {
        //for monitor
        redisKeeperServer.getKeeperMonitor().getMasterStats().increaseDefaultReplicationInputBytes(messageLength);
    }

    @Override
    public void masterDisconnected(Channel channel) {
        super.masterDisconnected(channel);
        getRedisMaster().setMasterState(MASTER_STATE.REDIS_REPL_NONE);

        long interval = System.currentTimeMillis() - connectedTime;
        long scheduleTime = masterConnectRetryDelaySeconds * 1000 - interval;
        if (scheduleTime < 0) {
            scheduleTime = 0;
        }
        logger.info("[masterDisconnected][reconnect after {} ms]{}", scheduleTime, this);
        scheduleReconnect((int) scheduleTime);
    }

    @Override
    protected void doWhenCannotPsync() {
        // close and reconnect later by masterDisconnect()
        disconnectWithMaster();
    }

    @Override
    protected void doStop() throws Exception {
        //put none immediately
        getRedisMaster().setMasterState(MASTER_STATE.REDIS_REPL_NONE);
        super.doStop();

    }

    @Override
    protected void doDispose() throws Exception {
        tryCloseCurrentPsync();
        super.doDispose();
    }

    private void tryCloseCurrentPsync() {
        Psync psync = currentPsync.get();
        if (null != psync) {
            try {
                psync.close();
            } catch (IOException e) {
                logger.info("[tryCloseCurrentPsync][fail]", e);
            }
        }
    }

    public void setMasterConnectRetryDelaySeconds(int masterConnectRetryDelaySeconds) {
        this.masterConnectRetryDelaySeconds = masterConnectRetryDelaySeconds;
    }

    @Override
    public void stopReplication() {
        super.stopReplication();

        cancelReplConf();
    }

    private void scheduleReplconf() {

        if (logger.isInfoEnabled()) {
            logger.info("[scheduleReplconf]" + this);
        }

        cancelReplConf();

        replConfFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {

                logger.debug("[run][send ack]{}", masterChannel);

                Command<Object> command = createReplConf();
                command.execute();

            }

        }, 0, REPLCONF_INTERVAL_MILLI, TimeUnit.MILLISECONDS);
    }

    protected void cancelReplConf() {

        if (replConfFuture != null) {
            replConfFuture.cancel(true);
            replConfFuture = null;
        }
    }

    protected Command<Object> createReplConf() {

        return new Replconf(clientPool, ReplConfType.ACK, scheduled, String.valueOf(redisMaster.getCurrentReplicationStore().getEndOffset()));
    }

    @Override
    protected void psyncFail(Throwable cause) {

        logger.info("[psyncFail][close channel, wait for reconnect]" + this, cause);
        disconnectWithMaster();
    }

    @Override
    protected Psync createPsync() {

        Psync psync;
        if (redisKeeperServer.getRedisKeeperServerState().keeperState().isBackup()) {
            psync = new PartialOnlyPsync(clientPool, redisMaster.masterEndPoint(), redisMaster.getReplicationStoreManager(), scheduled);
        } else {
            psync = new DefaultPsync(clientPool, redisMaster.masterEndPoint(), redisMaster.getReplicationStoreManager(), scheduled);
        }

        psync.addPsyncObserver(this);
        psync.addPsyncObserver(redisKeeperServer);

        tryCloseCurrentPsync();
        currentPsync.set(psync);

        return psync;
    }

    @Override
    public PARTIAL_STATE partialState() {
        return partialState;
    }

    @Override
    public void reconnectMaster() {
        disconnectWithMaster(); //DefaultRedisMasterReplication will reconnect master automatically.
    }

    @Override
    protected void doBeginWriteRdb(EofType eofType, long masterRdbOffset) throws IOException {

        redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_TRANSFER);

        partialState = PARTIAL_STATE.FULL;
        redisMaster.getCurrentReplicationStore().getMetaStore().setMasterAddress((DefaultEndPoint) redisMaster.masterEndPoint());
    }

    @Override
    protected void doEndWriteRdb() {
        logger.info("[doEndWriteRdb]{}", this);
        redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
        scheduleReplconf();

    }

    @Override
    protected void doOnContinue() {

        logger.info("[doOnContinue]{}", this);
        redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
        try {
            redisMaster.getCurrentReplicationStore().getMetaStore().setMasterAddress((DefaultEndPoint) redisMaster.masterEndPoint());
        } catch (IOException e) {
            logger.error("[doOnContinue]" + this, e);
        }

        scheduleReplconf();
        partialState = PARTIAL_STATE.PARTIAL;
        redisKeeperServer.getRedisKeeperServerState().initPromotionState();
    }

    @Override
    protected void doReFullSync() {
        redisKeeperServer.getRedisKeeperServerState().initPromotionState();
    }

    @Override
    protected void doOnFullSync(long masterRdbOffset) {

        try {
            logger.info("[doOnFullSync]{}", this);
            RdbDumper rdbDumper = new RedisMasterReplicationRdbDumper(this, redisKeeperServer, resourceManager);
            setRdbDumper(rdbDumper);
            redisKeeperServer.setRdbDumper(rdbDumper, true);
        } catch (SetRdbDumperException e) {
            //impossible to happen
            logger.error("[doOnFullSync][impossible to happen]", e);
        }
    }

    @Override
    protected String getSimpleName() {
        return "DefRep";
    }
}
