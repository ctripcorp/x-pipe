package com.ctrip.xpipe.redis.console.redis;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.console.notifier.ShardEventHandler;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSentinelCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.checker.resource.Resource.KEYED_NETTY_CLIENT_POOL;
import static com.ctrip.xpipe.redis.checker.resource.Resource.REDIS_COMMAND_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */

@Component
@Lazy
public class DefaultSentinelManager implements SentinelManager, ShardEventHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final int LONG_SENTINEL_COMMAND_TIMEOUT = 2000;

    @Resource(name = KEYED_NETTY_CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedClientPool;

    @Resource(name = REDIS_COMMAND_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Override
    public void handleShardDelete(ShardEvent shardEvent) {
        // remove shard sentinel monitors

        ClusterType clusterType = shardEvent.getClusterType();
        if (null != clusterType && clusterType.supportMultiActiveDC()) {
            // not support remove sentinel for multi active dc cluster temporarily
            return;
        }

        String clusterId = shardEvent.getClusterName(), shardId = shardEvent.getShardName();

        String sentinelMonitorName = shardEvent.getShardMonitorName();

        String allSentinels = shardEvent.getShardSentinels();

        logger.info("[removeShardSentinelMonitors]removeSentinelMonitor cluster:{}, shard:{}, masterName:{}, sentinelAddress:{}",
                clusterId, shardId, sentinelMonitorName, allSentinels);

        if(checkEmpty(sentinelMonitorName, allSentinels)) {
            return;
        }

        List<InetSocketAddress> sentinels = IpUtils.parse(allSentinels);
        List<Sentinel> realSentinels = getRealSentinels(sentinels, sentinelMonitorName);
        if(realSentinels == null) {
            logger.warn("[removeShardSentinelMonitors]real sentinels null");
            return;
        }

        logger.info("[removeShardSentinelMonitors]realSentinels: {}", realSentinels);

        for(Sentinel sentinel : realSentinels) {

            removeSentinelMonitor(sentinel, sentinelMonitorName);
        }
    }

    @Override
    public HostPort getMasterOfMonitor(Sentinel sentinel, String sentinelMonitorName) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));
        AbstractSentinelCommand.SentinelMaster sentinelMaster = new AbstractSentinelCommand
                .SentinelMaster(clientPool, scheduled, sentinelMonitorName);
        silentCommand(sentinelMaster);

        HostPort result = null;
        try {
            result = sentinelMaster.execute().get();
            logger.info("[getMasterOfMonitor] sentinel master {} from {} : {}", sentinelMonitorName, sentinel, result);
        } catch (Exception e) {
            logger.error("[getMasterOfMonitor] sentinel master {} from {} : {}", sentinelMonitorName, sentinel, e.getMessage());
        }
        return result;
    }

    @Override
    public void removeSentinelMonitor(Sentinel sentinel, String sentinelMonitorName) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));

        AbstractSentinelCommand.SentinelRemove sentinelRemove = new AbstractSentinelCommand
                .SentinelRemove(clientPool, sentinelMonitorName, scheduled);
        silentCommand(sentinelRemove);
        try {
            String result = sentinelRemove.execute().get();
            logger.info("[removeSentinels] sentinel remove {} from {} : {}", sentinelMonitorName, sentinel, result);

        } catch (Exception e) {
            logger.error("[removeSentinels] sentinel remove {} from {} : {}", sentinelMonitorName, sentinel, e.getMessage());
        }
    }

    @Override
    public String infoSentinel(Sentinel sentinel) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));

        InfoCommand infoCommand = new InfoCommand(clientPool, InfoCommand.INFO_TYPE.SENTINEL, scheduled);
        infoCommand.setCommandTimeoutMilli(LONG_SENTINEL_COMMAND_TIMEOUT);
        silentCommand(infoCommand);
        try {
            return infoCommand.execute().get();
        } catch (Exception e) {
            logger.error("[infoSentinel] " + sentinel, e);
        }
        return null;
    }

    @Override
    public void monitorMaster(Sentinel sentinel, String sentinelMonitorName, HostPort master, int quorum) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));
        try {
            AbstractSentinelCommand.SentinelMonitor command = new AbstractSentinelCommand.SentinelMonitor(clientPool,
                    scheduled, sentinelMonitorName, master, quorum);
            silentCommand(command);
            String result=command.execute().get();
            logger.info("[monitor] sentinel monitor {} for {} : {}", sentinelMonitorName, sentinel, result);
        } catch (Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof CommandTimeoutException) {
                logger.error("[monitor] sentinel monitor {} for {} : {}", sentinelMonitorName, sentinel, e.getMessage());
            } else {
                logger.error("[monitor] sentinel monitor {} for {}", sentinelMonitorName, sentinel, e);
            }
        }
    }

    @Override
    public List<HostPort> slaves(Sentinel sentinel, String sentinelMonitorName) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));
        try {
            AbstractSentinelCommand.SentinelSlaves command = new AbstractSentinelCommand.SentinelSlaves(clientPool,
                    scheduled, sentinelMonitorName);
            command.setCommandTimeoutMilli(LONG_SENTINEL_COMMAND_TIMEOUT);
            silentCommand(command);
            return command.execute().get();
        } catch (Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof CommandTimeoutException) {
                logger.error("[slaves] sentinel slaves {} from {} : {}", sentinelMonitorName, sentinel, e.getMessage());
            } else {
                logger.error("[slaves] sentinel slaves {} from {} : {}", sentinelMonitorName, sentinel, e);
            }
        }
        return Lists.newArrayList();
    }

    @Override
    public void reset(Sentinel sentinel, String sentinelMonitorName) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));
        try {
            new AbstractSentinelCommand.SentinelReset(clientPool, sentinelMonitorName, scheduled).execute().get();
            logger.info("[reset] sentinel reset {} for {}", sentinelMonitorName, sentinel);
        } catch (Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof CommandTimeoutException) {
                logger.error("[reset] sentinel reset {} for {} : {}", sentinelMonitorName, sentinel, e.getMessage());
            } else {
                logger.error("[reset] sentinel reset {} for {} : {}", sentinelMonitorName, sentinel, e);
            }
        }
    }

    @Override
    public void sentinelSet(Sentinel sentinel, String sentinelMonitorName, String[] configs) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));
        try {
            new AbstractSentinelCommand.SentinelSet(clientPool, scheduled, sentinelMonitorName, configs).execute().get();
            logger.info("[sentinelSet] sentinel set {} {} for {}", sentinelMonitorName, Arrays.toString(configs), sentinel);
        } catch (Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof CommandTimeoutException) {
                logger.error("[sentinelSet] sentinel set {} {} for {}, errMsg: {}", sentinelMonitorName, Arrays.toString(configs), sentinel, e.getMessage());
            } else {
                logger.error("[sentinelSet] sentinel set {} {} for {}", sentinelMonitorName, Arrays.toString(configs), sentinel, e);
            }
        }
    }

    @Override
    public void sentinelConfigSet(Sentinel sentinel, String configName, String configValue) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));
        try {
            new AbstractSentinelCommand.SentinelConfigSet(clientPool, scheduled, configName, configValue).execute().get();
            logger.info("[sentinelConfigSet] sentinel config set {} {} for {}", configName, configValue, sentinel);
        } catch (Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof CommandTimeoutException) {
                logger.error("[sentinelConfigSet] sentinel config set {} {} for {}, errMsg: {}", configName, configValue, sentinel, e.getMessage());
            } else {
                logger.error("[sentinelConfigSet] sentinel config set {} {} for {}", configName, configValue, sentinel, e);
            }
        }
    }

    private boolean checkEmpty(String sentinelMonitorName, String allSentinels) {

        if(StringUtil.isEmpty(sentinelMonitorName)){
            logger.warn("[checkEmpty]sentinelMonitorName empty, exit!");
            return true;
        }

        if(StringUtil.isEmpty(allSentinels)){
            logger.warn("[checkEmpty]allSentinels empty, exit!");
            return true;
        }
        return false;
    }

    @VisibleForTesting
    protected List<Sentinel> getRealSentinels(List<InetSocketAddress> sentinels, String sentinelMonitorName) {

        List<Sentinel> realSentinels = null;

        for(InetSocketAddress sentinelAddress: sentinels){

            SimpleObjectPool<NettyClient> clientPool = keyedClientPool.getKeyPool(new DefaultEndPoint(sentinelAddress));
            AbstractSentinelCommand.Sentinels sentinelsCommand = new AbstractSentinelCommand
                    .Sentinels(clientPool, sentinelMonitorName, scheduled);
            silentCommand(sentinelsCommand);
            try {
                realSentinels = sentinelsCommand.execute().get();
                logger.info("[getRealSentinels]sentinel sentinels from {} : {}", sentinelAddress, realSentinels);
                if(null != realSentinels){
                    realSentinels.add(
                            new Sentinel(sentinelAddress.toString(),
                                    sentinelAddress.getHostString(),
                                    sentinelAddress.getPort()
                            ));
                    logger.info("[getRealSentinels]sentinel sentinels {} : {}",
                            sentinelMonitorName, realSentinels);
                    break;
                }
            } catch (Exception e) {
                logger.warn("[getRealSentinels]sentinel sentinels {} from {}", sentinelMonitorName, sentinelAddress, e);
            }
        }


        return realSentinels;
    }

    private void silentCommand(AbstractRedisCommand command) {
        command.logRequest(false);
        command.logResponse(false);
    }

    @VisibleForTesting
    public DefaultSentinelManager setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }

    @VisibleForTesting
    public DefaultSentinelManager setKeyedClientPool(XpipeNettyClientKeyedObjectPool keyedClientPool) {
        this.keyedClientPool = keyedClientPool;
        return this;
    }
}
