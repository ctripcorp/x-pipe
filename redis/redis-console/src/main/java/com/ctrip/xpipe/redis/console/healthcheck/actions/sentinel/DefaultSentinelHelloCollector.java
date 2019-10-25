package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.session.DefaultRedisSessionManager;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.redis.SentinelManager;
import com.ctrip.xpipe.redis.console.resources.MasterNotFoundException;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@Component
public class DefaultSentinelHelloCollector implements SentinelHelloCollector {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSentinelHelloCollector.class);

    private static final String SENTINEL_TYPE = "sentinel";

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private DefaultRedisSessionManager sessionManager;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private SentinelManager sentinelManager;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;


    @Override
    public void onAction(SentinelActionContext context) {
        collect(context);
    }

    @Override
    public void stopWatch(HealthCheckAction action) {

    }

    private void collect(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getRedisInstanceInfo();
        if(info.isInActiveDc()) {
            logger.warn("[not in backup dc][{}] quit", info.getHostPort());
            return;
        }
        Set<SentinelHello> hellos = context.getResult();
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();
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
            Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());
            try {
                List<HostPort> slaves = sentinelManager.slaves(sentinel, sentinelMonitorName);
                boolean shoudReset = false;
                String reason = null;
                Set<HostPort> keepers = new HashSet<>();

                for (HostPort currentSlave : slaves) {

                    if(allKeepers.contains(currentSlave)){
                        keepers.add(currentSlave);
                    }

                    Pair<String, String> clusterShard = metaCache.findClusterShard(currentSlave);
                    if (clusterShard == null) {
                        if (isKeeperOrDead(currentSlave)) {
                            shoudReset = true;
                            reason = String.format("[%s]keeper or dead, current:%s,%s, but no clustershard", currentSlave, clusterId, shardId);
                        } else {
                            String message = String.format("sentinel monitors redis %s not in xpipe", currentSlave.toString());
                            alertManager.alert(clusterId, shardId, currentSlave, ALERT_TYPE.SENTINEL_MONITOR_REDUNDANT_REDIS, message);
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
                    sentinelManager.reset(sentinel, sentinelMonitorName);
                }
            } catch (Exception e) {
                logger.error("[doAction][checkReset]" + hello, e);
            }
        });
    }

    @VisibleForTesting
    protected boolean isKeeperOrDead(HostPort hostPort) {

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Object> role = new AtomicReference<>();

        try {
            RedisSession redisSession = sessionManager.findOrCreateSession(hostPort);
            redisSession.role(new RedisSession.RollCallback() {
                @Override
                public void role(String roleDesc) {
                    role.set(roleDesc);
                    latch.countDown();
                }

                @Override
                public void fail(Throwable e) {
                    logger.error("[isKeeperOrDead][fail]" + hostPort, e);
                    role.set(e);
                    latch.countDown();
                }
            });
        } catch (Exception e) {
            role.set(e);
            logger.error("[isKeeperOrDead]" + hostPort, e);
        }

        try {
            latch.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("[isKeeperOrDead]latch await error: {}", e);
        }

        if (role.get() instanceof String && Server.SERVER_ROLE.KEEPER.sameRole((String) role.get())) {
            return true;
        }
        if (role.get() instanceof CommandExecutionException || role.get() instanceof CommandTimeoutException) {
            return true;
        }
        logger.info("[isKeeperOrDead] role: {}", role.get());
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
                try {
                    CatEventMonitor.DEFAULT.logEvent(SENTINEL_TYPE, "[del]" + hello);
                    sentinelManager.removeSentinelMonitor(new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort()), hello.getMonitorName());
                } catch (Exception e) {
                    logger.error("[doAction][delete]" + hello, e);
                }
            }));
        }

        if(toAdd != null){
            toAdd.forEach((hello) -> {
                HostPort sentinelAddr = hello.getSentinelAddr();
                try {
                    Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());
                    boolean doAdd = true;
                    try {
                        HostPort masterHostPort = sentinelManager.getMasterOfMonitor(sentinel, hello.getMonitorName());
                        if (hello.getMasterAddr().equals(masterHostPort)) {
                            doAdd = false;
                            logger.info("[doAction][already exist]{}, {}", masterHostPort, hello.getSentinelAddr());
                        } else {
                            sentinelManager.removeSentinelMonitor(sentinel, hello.getMonitorName());
                        }
                    } catch (Exception e) {
                        //ingnore
                    }
                    if (doAdd) {
                        CatEventMonitor.DEFAULT.logEvent(SENTINEL_TYPE, "[add]" + hello);
                        sentinelManager.monitorMaster(sentinel, hello.getMonitorName(), hello.getMasterAddr(), quorumConfig.getQuorum());
                    }
                } catch (Exception e) {
                    logger.error("[doAction][add]" + hello, e);
                }
            });
        }

    }

    private boolean checkMasterConsistent(HostPort masterAddr, Set<SentinelHello> hellos) {

        if (hellos.isEmpty()) {
            RoleCommand roleCommand = new RoleCommand(masterAddr.getHost(), masterAddr.getPort(), false, scheduled);
            try {
                Role role = roleCommand.execute().get(2, TimeUnit.SECONDS);
                if (!(role instanceof MasterRole)) {
                    return false;
                }
            } catch (Exception e) {
                logger.error("[checkMasterConsistent] check redis role err", e);
                return false;
           }
        }

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

    @VisibleForTesting
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

    @VisibleForTesting
    public void setSessionManager(DefaultRedisSessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @VisibleForTesting
    public void setAlertManager(AlertManager alertManager) {
        this.alertManager = alertManager;
    }

    @VisibleForTesting
    protected DefaultSentinelHelloCollector setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }

    @VisibleForTesting
    protected DefaultSentinelHelloCollector setConsoleConfig(ConsoleConfig consoleConfig) {
        this.consoleConfig = consoleConfig;
        return this;
    }
}
