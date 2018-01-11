package com.ctrip.xpipe.redis.console.health.sentinel;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.DefaultRedisSessionManager;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.resources.MasterNotFoundException;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Sets;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnectionException;
import com.lambdaworks.redis.sentinel.api.StatefulRedisSentinelConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
@Component
@Lazy
public class DefaultSentinelCollector implements SentinelCollector {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final String SENTINEL_TYPE = "sentinel";

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private DefaultRedisSessionManager sessionManager;

    @Override
    public void collect(SentinelSample sentinelSample) {

        Set<SentinelHello> hellos = sentinelSample.getHellos();
        String clusterId = sentinelSample.getSamplePlan().getClusterId();
        String shardId = sentinelSample.getSamplePlan().getShardId();
        String sentinelMonitorName = metaCache.getSentinelMonitorName(clusterId, shardId);
        Set<HostPort> masterDcSentinels = metaCache.getActiveDcSentinels(clusterId, shardId);
        QuorumConfig quorumConfig = consoleConfig.getDefaultSentinelQuorumConfig();
        HostPort masterAddr = null;
        try{
            masterAddr = metaCache.findMaster(clusterId, shardId);
        } catch (MasterNotFoundException e) {
            logger.error("[collect]" + e.getMessage(), e);
        }

        logger.debug("[collect]{},{},{}", clusterId, shardId, hellos);

        //check delete
        Set<SentinelHello> toDelete = checkAndDelete(sentinelMonitorName, masterDcSentinels, hellos, quorumConfig);
        //checkReset
        checkReset(clusterId, shardId, sentinelMonitorName, hellos);
        //check add
        Set<SentinelHello> toAdd = checkToAdd(clusterId, shardId, sentinelMonitorName, masterDcSentinels, hellos, masterAddr, quorumConfig);

        doAction(toDelete, toAdd, quorumConfig);
    }

    protected void checkReset(String clusterId, String shardId, String sentinelMonitorName, Set<SentinelHello> hellos) {

        Set<HostPort> allKeepers = metaCache.allKeepers();

        hellos.forEach((hello) -> {
            HostPort sentinelAddr = hello.getSentinelAddr();
            RedisClient redisConnection = null;
            try {
                redisConnection = sessionManager.findRedisConnection(sentinelAddr.getHost(), sentinelAddr.getPort());
                StatefulRedisSentinelConnection<String, String> connection = redisConnection.connectSentinel();
                List<Map<String, String>> slaves = connection.sync().slaves(sentinelMonitorName);

                boolean shoudReset = false;
                String reason = null;
                Set<HostPort> keepers = new HashSet<>();

                for (Map<String, String> slave : slaves) {

                    String host = slave.get("ip");
                    int port = Integer.parseInt(slave.get("port"));
                    HostPort currentSlave = new HostPort(host, port);

                    if(allKeepers.contains(currentSlave)){
                        keepers.add(currentSlave);
                    }

                    Pair<String, String> clusterShard = metaCache.findClusterShard(currentSlave);
                    if (clusterShard == null) {
                        if (isKeeperOrDead(host, port)) {
                            shoudReset = true;
                            reason = String.format("[%s]keeper or dead, current:%s,%s, but no clustershard", currentSlave, clusterId, shardId);
                        }
                        continue;
                    }
                    if (!ObjectUtils.equals(clusterId, clusterShard.getKey()) || !ObjectUtils.equals(shardId, clusterShard.getValue())) {
                        shoudReset = true;
                        reason = String.format("[%s], current:%s,%s, but meta:%s:%s", currentSlave, clusterId, shardId, clusterShard.getKey(), clusterShard.getValue());
                        break;
                    }
                }

                if(!shoudReset && keepers.size() >= 2){
                    shoudReset = true;
                    reason = String.format("%s,%s, has %d keepers:%s", clusterId, shardId, keepers.size(), keepers);
                }

                if (shoudReset) {
                    logger.info("[reset][sentinelAddr]{}, {}, {}", sentinelAddr, sentinelMonitorName, reason);
                    EventMonitor.DEFAULT.logEvent(SENTINEL_TYPE,
                            String.format("[%s]%s,%s", ALERT_TYPE.SENTINEL_RESET, sentinelAddr, reason));
                    connection.sync().reset(sentinelMonitorName);
                }
            } catch (Exception e) {
                logger.error("[doAction][checkReset]" + hello, e);
            } finally {
                if (redisConnection != null) {
                    redisConnection.shutdown();
                }
            }
        });
    }

    private boolean isKeeperOrDead(String host, int port) {

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Object> role = new AtomicReference<>();

        try {
            RedisSession redisSession = sessionManager.findOrCreateSession(host, port);
            redisSession.role(new RedisSession.RollCallback() {
                @Override
                public void role(String roleDesc) {
                    role.set(roleDesc);
                    latch.countDown();
                }

                @Override
                public void fail(Throwable e) {
                    logger.error("[fail]" + host + ":" + port, e);
                    role.set(e);
                    latch.countDown();
                }
            });
        } catch (Exception e) {
            role.set(e);
            logger.error("[isKeeperOrDead]" + host + ":" + port, e);
        }

        try {
            latch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.debug("[isKeeperOrDead]latch await error: {}", e);
        }

        if (role.get() instanceof String && Server.SERVER_ROLE.KEEPER.sameRole((String) role.get())) {
            return true;
        }
        if (role.get() instanceof RedisConnectionException) {
            return true;
        }
        return false;
    }

    private void doAction(Set<SentinelHello> toDelete, Set<SentinelHello> toAdd, QuorumConfig quorumConfig) {

        if ((toDelete == null || toDelete.size() == 0) && (toAdd == null || toAdd.size() == 0)) {
            return;
        }
        if (toAdd != null && toAdd.size() > 0) {
            logger.info("[doAction][add]{}", toAdd);
        }

        if (toDelete != null && toDelete.size() > 0) {
            logger.info("[doAction][del]{}", toDelete);
        }

        if(toDelete != null){
            toDelete.forEach((hello -> {
                HostPort sentinelAddr = hello.getSentinelAddr();
                RedisClient redisConnection = null;
                try {
                    CatEventMonitor.DEFAULT.logEvent(SENTINEL_TYPE, "[del]" + hello);
                    redisConnection = sessionManager.findRedisConnection(sentinelAddr.getHost(), sentinelAddr.getPort());
                    redisConnection.connectSentinel().sync().remove(hello.getMonitorName());
                } catch (Exception e) {
                    logger.error("[doAction][delete]" + hello, e);
                } finally {
                    if (redisConnection != null) {
                        redisConnection.shutdown();
                    }
                }
            }));
        }

        if(toAdd != null){
            toAdd.forEach((hello) -> {
                HostPort sentinelAddr = hello.getSentinelAddr();
                RedisClient redisConnection = null;
                try {
                    // TODO: Re-use connection instead of creating them each time
                    redisConnection = sessionManager.findRedisConnection(sentinelAddr.getHost(), sentinelAddr.getPort());

                    boolean doAdd = true;
                    try {
                        Map<String, String> map = redisConnection.connectSentinel().sync().master(hello.getMonitorName());
                        if (equals(hello.getMonitorName(), hello.getMasterAddr(), map)) {
                            doAdd = false;
                            logger.info("[doAction][already exist]{}, {}", map, hello.getSentinelAddr());
                        } else {
                            redisConnection.connectSentinel().sync().remove(hello.getMonitorName());
                        }
                    } catch (Exception e) {
                        //ingnore
                    }
                    if (doAdd) {
                        CatEventMonitor.DEFAULT.logEvent(SENTINEL_TYPE, "[add]" + hello);
                        redisConnection.connectSentinel().sync().monitor(
                                hello.getMonitorName(),
                                hello.getMasterAddr().getHost(),
                                hello.getMasterAddr().getPort(),
                                quorumConfig.getQuorum()
                        );
                    }
                } catch (Exception e) {
                    logger.error("[doAction][add]" + hello, e);
                } finally {
                    if (redisConnection != null) {
                        redisConnection.shutdown();
                    }
                }
            });
        }

    }

    private boolean equals(String monitorName, HostPort masterAddr, Map<String, String> map) {

        if (!map.get("name").equals(monitorName)) {
            return false;
        }
        if (!map.get("ip").equals(masterAddr.getHost())) {
            return false;
        }
        int port = Integer.parseInt(map.get("port"));
        if (!(masterAddr.getPort() == port)) {
            return false;
        }

        return true;

    }

    private boolean checkMasterConsistent(HostPort masterAddr, Set<SentinelHello> hellos) {

        for (SentinelHello hello : hellos) {

            if (!hello.getMasterAddr().equals(masterAddr)) {
                return false;
            }
        }
        return true;
    }

    protected Set<SentinelHello> checkAndDelete(String sentinelMonitorName, Set<HostPort> masterDcSentinels, Set<SentinelHello> hellos, QuorumConfig quorumConfig) {

        Set<SentinelHello> toDelete = new HashSet<>();

        hellos.forEach((hello) -> {

            if (!hello.getMonitorName().equals(sentinelMonitorName)) {
                toDelete.add(hello);
            }
        });

        hellos.forEach((hello) -> {
            HostPort sentinel = hello.getSentinelAddr();
            if (!masterDcSentinels.contains(sentinel)) {
                toDelete.add(hello);
            }

        });

        toDelete.forEach((delete) -> {
            hellos.remove(delete);
        });

        int toRemove = hellos.size() - quorumConfig.getTotal();
        if (toRemove > 0) {
            int i = 0;
            for (SentinelHello hello : hellos) {
                i++;
                toDelete.add(hello);
                if (i >= toRemove) {
                    break;
                }
            }
        }

        toDelete.forEach((delete) -> {
            hellos.remove(delete);
        });
        return toDelete;
    }

    protected Set<SentinelHello> checkToAdd(String clusterId, String shardId, String sentinelMonitorName, Set<HostPort> masterDcSentinels, Set<SentinelHello> hellos, HostPort masterAddr, QuorumConfig quorumConfig) {

        if(masterAddr == null){
            logger.warn("[checkToAdd][no monitor name]{}, {}", clusterId, shardId);
            return Sets.newHashSet();
        }

        if (StringUtil.isEmpty(sentinelMonitorName)) {
            logger.warn("[checkToAdd][no monitor name]{}, {}", clusterId, shardId);
            return Sets.newHashSet();
        }

        if (hellos.size() >= quorumConfig.getTotal()) {
            return Sets.newHashSet();
        }

        if (!checkMasterConsistent(masterAddr, hellos)) {
            logger.info("[collect][master not consistent]{}, {}, {}", sentinelMonitorName, masterAddr, hellos);
            return Sets.newHashSet();
        }


        Set<HostPort> currentSentinels = new HashSet<>();
        hellos.forEach((hello -> currentSentinels.add(hello.getSentinelAddr())));

        Set<SentinelHello> toAdd = new HashSet<>();
        int toAddSize = quorumConfig.getTotal() - hellos.size();

        int i = 0;
        for (HostPort hostPort : masterDcSentinels) {
            if (!currentSentinels.contains(hostPort)) {
                i++;
                if(i > toAddSize){
                    break;
                }
                toAdd.add(new SentinelHello(hostPort, masterAddr, sentinelMonitorName));
            }
        }
        return toAdd;
    }

}
