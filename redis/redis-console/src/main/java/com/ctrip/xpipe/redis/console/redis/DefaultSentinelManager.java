package com.ctrip.xpipe.redis.console.redis;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.console.notifier.ShardEventHandler;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSentinelCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.protocal.pojo.SentinelMasterInstance;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.redis.checker.resource.Resource.REDIS_COMMAND_EXECUTOR;
import static com.ctrip.xpipe.redis.checker.resource.Resource.SENTINEL_KEYED_NETTY_CLIENT_POOL;

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

    @Resource(name = SENTINEL_KEYED_NETTY_CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedClientPool;

    @Resource(name = REDIS_COMMAND_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private MetricProxy metricProxy = MetricProxy.DEFAULT;

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

        ParallelCommandChain chain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        for (Sentinel sentinel : realSentinels) {
            chain.add(removeSentinelMonitor(sentinel, sentinelMonitorName));
        }
        chain.execute().getOrHandle(1000, TimeUnit.MILLISECONDS, throwable -> {
            logger.error("[removeShardSentinelMonitors]realSentinels: {}", realSentinels, throwable);
            return null;
        });

    }

    @Override
    public Command<SentinelMasterInstance> getMasterOfMonitor(Sentinel sentinel, String sentinelMonitorName) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));
        AbstractSentinelCommand.SentinelMaster sentinelMaster = new AbstractSentinelCommand
                .SentinelMaster(clientPool, scheduled, sentinelMonitorName);
        silentCommand(sentinelMaster);
        sentinelMaster.future().addListener(innerFuture -> {
            tryMetric("master", sentinel.getIp(), sentinel.getPort(), sentinelMonitorName, innerFuture.isSuccess());
            if (innerFuture.isSuccess()) {
                logger.info("[getMasterOfMonitor] sentinel master {} from {} : {}", sentinelMonitorName, sentinel, innerFuture.get());
            } else {
                logger.error("[getMasterOfMonitor] sentinel master {} from {} : {}", sentinelMonitorName, sentinel, innerFuture.cause().getMessage());
            }
        });
        return sentinelMaster;
    }

    @Override
    public Command<String> removeSentinelMonitor(Sentinel sentinel, String sentinelMonitorName) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));

        AbstractSentinelCommand.SentinelRemove sentinelRemove = new AbstractSentinelCommand
                .SentinelRemove(clientPool, sentinelMonitorName, scheduled);
        silentCommand(sentinelRemove);
        sentinelRemove.future().addListener(innerFuture -> {
            tryMetric("remove", sentinel.getIp(), sentinel.getPort(), sentinelMonitorName, innerFuture.isSuccess());
            if (innerFuture.isSuccess()) {
                logger.info("[removeSentinels] sentinel remove {} from {} : {}", sentinelMonitorName, sentinel, innerFuture.get());
            } else {
                logger.error("[removeSentinels] sentinel remove {} from {} : {}", sentinelMonitorName, sentinel, innerFuture.cause().getMessage());
            }
        });
        return sentinelRemove;
    }

    @Override
    public Command<String> infoSentinel(Sentinel sentinel) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));

        InfoCommand infoCommand = new InfoCommand(clientPool, InfoCommand.INFO_TYPE.SENTINEL, scheduled);
        infoCommand.setCommandTimeoutMilli(LONG_SENTINEL_COMMAND_TIMEOUT);
        silentCommand(infoCommand);
        infoCommand.future().addListener(innerFuture -> {
            tryMetric("info", sentinel.getIp(), sentinel.getPort(), null, innerFuture.isSuccess());
            if (!innerFuture.isSuccess()) {
                logger.error("[infoSentinel] " + sentinel, innerFuture.cause());
            }
        });
        return infoCommand;
    }

    @Override
    public Command<String> monitorMaster(Sentinel sentinel, String sentinelMonitorName, HostPort master, int quorum) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));

        AbstractSentinelCommand.SentinelMonitor command = new AbstractSentinelCommand.SentinelMonitor(clientPool,
                scheduled, sentinelMonitorName, master, quorum);
        silentCommand(command);
        command.future().addListener(innerFuture -> {
            tryMetric("monitor", sentinel.getIp(), sentinel.getPort(), sentinelMonitorName, innerFuture.isSuccess());
            if (innerFuture.isSuccess()) {
                logger.info("[monitor] sentinel monitor {} for {} : {}", sentinelMonitorName, sentinel, innerFuture.get());
            } else {
                if (ExceptionUtils.getRootCause(innerFuture.cause()) instanceof CommandTimeoutException) {
                    logger.error("[monitor] sentinel monitor {} for {} : {}", sentinelMonitorName, sentinel, innerFuture.cause().getMessage());
                } else {
                    logger.error("[monitor] sentinel monitor {} for {}", sentinelMonitorName, sentinel, innerFuture.cause());
                }
            }
        });
        return command;
    }

    @Override
    public Command<List<HostPort>> slaves(Sentinel sentinel, String sentinelMonitorName) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));

        AbstractSentinelCommand.SentinelSlaves command = new AbstractSentinelCommand.SentinelSlaves(clientPool,
                scheduled, sentinelMonitorName);
        command.setCommandTimeoutMilli(LONG_SENTINEL_COMMAND_TIMEOUT);
        silentCommand(command);
        command.future().addListener(innerFuture -> {
            tryMetric("slaves", sentinel.getIp(), sentinel.getPort(), sentinelMonitorName, innerFuture.isSuccess());
            if (!innerFuture.isSuccess()) {
                if (ExceptionUtils.getRootCause(innerFuture.cause()) instanceof CommandTimeoutException) {
                    logger.error("[slaves] sentinel slaves {} from {} : {}", sentinelMonitorName, sentinel, innerFuture.cause().getMessage());
                } else {
                    logger.error("[slaves] sentinel slaves {} from {} : {}", sentinelMonitorName, sentinel, innerFuture.cause());
                }
            }
        });
        return command;
    }

    @Override
    public Command<Long> reset(Sentinel sentinel, String sentinelMonitorName) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));

        AbstractSentinelCommand.SentinelReset sentinelReset = new AbstractSentinelCommand.SentinelReset(clientPool, sentinelMonitorName, scheduled);
        silentCommand(sentinelReset);
        sentinelReset.future().addListener(innerFuture -> {
            tryMetric("reset", sentinel.getIp(), sentinel.getPort(), sentinelMonitorName, innerFuture.isSuccess());
            if (innerFuture.isSuccess()) {
                logger.info("[reset] sentinel reset {} for {}", sentinelMonitorName, sentinel);
            } else {
                if (ExceptionUtils.getRootCause(innerFuture.cause()) instanceof CommandTimeoutException) {
                    logger.error("[reset] sentinel reset {} for {} : {}", sentinelMonitorName, sentinel, innerFuture.cause().getMessage());
                } else {
                    logger.error("[reset] sentinel reset {} for {} : {}", sentinelMonitorName, sentinel, innerFuture.cause());
                }
            }
        });
        return sentinelReset;
    }

    @Override
    public Command<String> sentinelSet(Sentinel sentinel, String sentinelMonitorName, String[] configs) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));

        AbstractSentinelCommand.SentinelSet sentinelSet = new AbstractSentinelCommand.SentinelSet(clientPool, scheduled, sentinelMonitorName, configs);
        silentCommand(sentinelSet);
        sentinelSet.future().addListener(innerFuture -> {
            tryMetric("set", sentinel.getIp(), sentinel.getPort(),null, innerFuture.isSuccess());
            if (innerFuture.isSuccess()) {
                logger.info("[sentinelSet] sentinel set {} {} for {}", sentinelMonitorName, Arrays.toString(configs), sentinel);
            } else {
                if (ExceptionUtils.getRootCause(innerFuture.cause()) instanceof CommandTimeoutException) {
                    logger.error("[sentinelSet] sentinel set {} {} for {}, errMsg: {}", sentinelMonitorName, Arrays.toString(configs), sentinel, innerFuture.cause().getMessage());
                } else {
                    logger.error("[sentinelSet] sentinel set {} {} for {}", sentinelMonitorName, Arrays.toString(configs), sentinel, innerFuture.cause());
                }
            }
        });
        return sentinelSet;
    }

    @Override
    public Command<String> sentinelConfigSet(Sentinel sentinel, String configName, String configValue) {
        SimpleObjectPool<NettyClient> clientPool = keyedClientPool
                .getKeyPool(new DefaultEndPoint(sentinel.getIp(), sentinel.getPort()));

        AbstractSentinelCommand.SentinelConfigSet sentinelConfigSet = new AbstractSentinelCommand.SentinelConfigSet(clientPool, scheduled, configName, configValue);
        silentCommand(sentinelConfigSet);
        sentinelConfigSet.future().addListener(innerFuture -> {
            tryMetric("config_set", sentinel.getIp(), sentinel.getPort(), null, innerFuture.isSuccess());
            if (innerFuture.isSuccess()) {
                logger.info("[sentinelConfigSet] sentinel config set {} {} for {}", configName, configValue, sentinel);
            } else {
                if (ExceptionUtils.getRootCause(innerFuture.cause()) instanceof CommandTimeoutException) {
                    logger.error("[sentinelConfigSet] sentinel config set {} {} for {}, errMsg: {}", configName, configValue, sentinel, innerFuture.cause().getMessage());
                } else {
                    logger.error("[sentinelConfigSet] sentinel config set {} {} for {}", configName, configValue, sentinel, innerFuture.cause());
                }
            }
        });
        return sentinelConfigSet;
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
            sentinelsCommand.future().addListener(innerFuture ->
                    tryMetric("sentinels", sentinelAddress.getHostString(), sentinelAddress.getPort(), sentinelMonitorName, innerFuture.isSuccess()));
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

    private void tryMetric(String cmd, String sentinelIp, int sentinelPort, String sentinelMonitorName, boolean success) {
        try {
            MetricData metricData = new MetricData("call.sentinel");
            metricData.setValue(1);
            metricData.setTimestampMilli(System.currentTimeMillis());
            metricData.addTag("cmd", cmd);
            metricData.addTag("sentinelAddr", String.format("%s:%d", sentinelIp, sentinelPort));
            metricData.addTag("status", success ? "SUCCESS" : "FAIL");
            metricData.addTag("sentinelMonitorName", sentinelMonitorName);
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (Throwable th) {
            logger.debug("[tryMetric] fail", th);
        }
    }

    private void silentCommand(AbstractRedisCommand command) {
        command.logRequest(false);
        command.logResponse(false);
    }

    @VisibleForTesting
    protected void setMetricProxy(MetricProxy metricProxy) {
        this.metricProxy = metricProxy;
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
