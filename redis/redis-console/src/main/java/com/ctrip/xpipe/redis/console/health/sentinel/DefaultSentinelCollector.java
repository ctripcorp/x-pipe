package com.ctrip.xpipe.redis.console.health.sentinel;

import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.DefaultRedisSessionManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Sets;
import com.lambdaworks.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
@Component
@Lazy
public class DefaultSentinelCollector implements SentinelCollector {

    private Logger logger = LoggerFactory.getLogger(getClass());

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
        HostPort masterAddr = metaCache.findMaster(clusterId, shardId);

        logger.debug("[collect]{},{},{}", clusterId, shardId, hellos);

        //check delete
        Set<SentinelHello> toDelete = checkAndDelete(sentinelMonitorName, masterDcSentinels, hellos, quorumConfig);

        //check add
        Set<SentinelHello> toAdd = checkToAdd(sentinelMonitorName, masterDcSentinels, hellos, masterAddr, quorumConfig);

        doAction(toDelete, toAdd, quorumConfig);
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
            CatEventMonitor.DEFAULT.logAlertEvent("[sentinel][delete]" + toDelete);
        }

        toDelete.forEach((hello -> {

            HostPort sentinelAddr = hello.getSentinelAddr();
            RedisClient redisConnection = null;
            try {
                CatEventMonitor.DEFAULT.logAlertEvent("[sentinel][del]" + hello);
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

        toAdd.forEach((hello) -> {
            HostPort sentinelAddr = hello.getSentinelAddr();
            RedisClient redisConnection = null;
            try {
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
                    CatEventMonitor.DEFAULT.logAlertEvent("[sentinel][add]" + hello);
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

    protected Set<SentinelHello> checkToAdd(String sentinelMonitorName, Set<HostPort> masterDcSentinels, Set<SentinelHello> hellos, HostPort masterAddr, QuorumConfig quorumConfig) {

        if (StringUtil.isEmpty(sentinelMonitorName)) {
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
                toAdd.add(new SentinelHello(hostPort, masterAddr, sentinelMonitorName));
            }

        }

        return toAdd;
    }

}
