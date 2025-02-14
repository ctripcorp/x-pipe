package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelInvalidSlaves;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.MasterSlavesInfo;
import com.ctrip.xpipe.redis.core.exception.SentinelsException;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.RedisInfo;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class ResetSentinels extends AbstractSentinelHelloCollectCommand {

    private SentinelManager sentinelManager;
    private MetaCache metaCache;
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;
    private ScheduledExecutorService scheduled;
    private ExecutorService resetExecutor;
    private CheckerConfig checkerConfig;
    private Map<HostPort, List<HostPort>> allSentinels = new ConcurrentHashMap<>();
    private Map<HostPort, List<HostPort>> availableSentinels = new ConcurrentHashMap<>();
    private Map<HostPort, SentinelInvalidSlaves> problemSentinels = new ConcurrentHashMap<>();
    private Map<HostPort, SentinelInvalidSlaves> shouldResetSentinels = new ConcurrentHashMap<>();
    private List<HostPort> masterSlaves = new ArrayList<>();

    public ResetSentinels(SentinelHelloCollectContext context, MetaCache metaCache,
                          XpipeNettyClientKeyedObjectPool keyedObjectPool,
                          ScheduledExecutorService scheduled, ExecutorService resetExecutor, SentinelManager sentinelManager,
                          CheckerConfig checkerConfig) {
        super(context);
        this.metaCache = metaCache;
        this.keyedObjectPool = keyedObjectPool;
        this.scheduled = scheduled;
        this.resetExecutor = resetExecutor;
        this.sentinelManager = sentinelManager;
        this.checkerConfig = checkerConfig;
    }

    @Override
    protected void doExecute() throws Throwable {
        checkReset();
        future().setSuccess();
    }

    protected void checkReset() {
        if (context.getToCheckReset().isEmpty())
            return;

        if (context.getToCheckReset().size() < checkerConfig.getDefaultSentinelQuorumConfig().getQuorum()) {
            logger.warn("[{}-{}][reset]collected sentinels less then quorum, ignore reset, hellos: {}", LOG_TITLE, context.getSentinelMonitorName(), context.getToCheckReset());
            return;
        }

        CommandFuture resetFuture = checkResetCommands().execute(resetExecutor);
        ScheduledFuture<?> resetTimeoutFuture = scheduled.schedule(() -> {
            resetFuture.cancel(true);
        }, 3000, TimeUnit.MILLISECONDS);
        resetFuture.addListener(commandFuture -> {
            if (!commandFuture.isCancelled())
                resetTimeoutFuture.cancel(true);
        });

    }

    SequenceCommandChain checkResetCommands() {
        Set<SentinelHello> hellos = context.getToCheckReset();
        SequenceCommandChain checkResetCommands = new SequenceCommandChain(false);
        checkResetCommands.add(getSentinelsSlaves(hellos));
        checkResetCommands.add(checkSentinelsSlaves(hellos));
        checkResetCommands.add(checkMaster());
        checkResetCommands.add(filterSentinelsToReset());
        checkResetCommands.add(resetSentinels());
        return checkResetCommands;
    }

    Command<Void> filterSentinelsToReset() {
        return new AbstractCommand<Void>() {
            @Override
            protected void doExecute() {
                if (problemSentinels.isEmpty())
                    future().setSuccess();
                else {
                    ParallelCommandChain checkCommands = new ParallelCommandChain(resetExecutor, false);
                    for (HostPort sentinel : problemSentinels.keySet()) {
                        Command<Boolean> shouldResetSentinel = shouldResetSentinel(sentinel);
                        shouldResetSentinel.future().addListener(shouldResetSentinelFuture -> {
                            if (shouldResetSentinelFuture.isSuccess() && shouldResetSentinelFuture.get())
                                shouldResetSentinels.put(sentinel, problemSentinels.get(sentinel));
                        });
                        checkCommands.add(shouldResetSentinel);
                    }
                    checkCommands.execute().addListener(future -> future().setSuccess());
                }
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "filterSentinelsToReset";
            }
        };
    }

    Command<Boolean> shouldResetSentinel(HostPort sentinel) {
        return new AbstractCommand<Boolean>() {
            @Override
            protected void doExecute() {
                future().setSuccess(shouldReset(sentinel));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "shouldResetSentinel";
            }
        };
    }

    Command<Void> checkMaster() {
        return new AbstractCommand<Void>() {
            @Override
            protected void doExecute() {
                if (problemSentinels.isEmpty())
                    future().setSuccess();
                else {
                    HostPort master = context.getTrueMasterInfo().getKey();
                    CheckMasterRoleAndSlaves checkMasterRoleAndSlaves = new CheckMasterRoleAndSlaves(keyedObjectPool.getKeyPool(new DefaultEndPoint(master.getHost(), master.getPort())), scheduled);
                    checkMasterRoleAndSlaves.execute().addListener(innerFuture -> {
                        if (innerFuture.isSuccess()) {
                            RedisInfo redisInfo = innerFuture.get();
                            if (redisInfo instanceof MasterSlavesInfo) {
                                MasterSlavesInfo masterSlavesInfo = (MasterSlavesInfo) innerFuture.get();
                                masterSlaves.addAll(masterSlavesInfo.getSlaves());

                                if (masterHasAllSlaves())
                                    future().setSuccess();
                                else
                                    future().setFailure(new SentinelsException(String.format("master: %s of %s lost slaves", master, context.getSentinelMonitorName())));

                            } else {
                                String errMsg = String.format("unexpected master info:%s, master:%s, shard:%s", redisInfo.getRole().name(), master, context.getSentinelMonitorName());
                                logger.warn("[{}-{}][reset]{}", LOG_TITLE, context.getSentinelMonitorName(), errMsg);
                                future().setFailure(new SentinelsException(errMsg));
                            }
                        } else {
                            String errMsg = String.format("info replication master %s of %s failed", master, context.getSentinelMonitorName());
                            logger.warn("[{}-{}][reset]{}", LOG_TITLE, context.getSentinelMonitorName(), errMsg, innerFuture.cause());
                            future().setFailure(new SentinelsException(errMsg));
                        }
                    });
                }
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "checkMaster";
            }
        };
    }

    Command<Void> resetSentinels() {
        return new AbstractCommand<Void>() {
            @Override
            protected void doExecute() {
                List<HostPort> resetSentinels = sentinelsToReset();
                if (!resetSentinels.isEmpty()) {
                    logger.info("[{}-{}]to reset sentinels: {}", LOG_TITLE, context.getSentinelMonitorName(), resetSentinels);
                    resetSentinels.forEach(sentinel -> {
                        CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.Reset", context.getSentinelMonitorName());
                        sentinelManager.reset(new Sentinel(sentinel.toString(), sentinel.getHost(), sentinel.getPort()), context.getSentinelMonitorName()).execute().getOrHandle(1000, TimeUnit.MILLISECONDS, throwable -> {
                            logger.error("[{}-{}][reset]{}", LOG_TITLE, context.getSentinelMonitorName(), sentinel, throwable);
                            return null;
                        });
                    });
                }
                future().setSuccess();
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "resetSentinels";
            }
        };
    }

    Command<Void> checkSentinelsSlaves(Set<SentinelHello> hellos) {
        return new AbstractCommand<Void>() {
            @Override
            protected void doExecute() {
                filterAvailableSentinels();
                if (overHalfSentinelsLostSlaves()) {
                    logger.warn("[{}-{}][reset]over half sentinels lost slaves: {}, ignore reset", LOG_TITLE, context.getSentinelMonitorName(), allSentinels);
                    future().setFailure(new SentinelsException("over half sentinels unavailable"));
                } else {
                    ParallelCommandChain checkCommands = new ParallelCommandChain(resetExecutor, false);
                    for (SentinelHello hello : hellos) {
                        HostPort sentinelAddr = hello.getSentinelAddr();
                        List<HostPort> slaves = allSentinels.get(sentinelAddr);
                        CheckSentinelSlaves checkSentinel = new CheckSentinelSlaves(slaves);
                        checkSentinel.future().addListener(checkSentinelFuture -> {
                            SentinelInvalidSlaves invalidSlaves = checkSentinelFuture.get();
                            if (invalidSlaves.hasInvalidSlaves())
                                problemSentinels.put(sentinelAddr, invalidSlaves);
                        });
                        checkCommands.add(checkSentinel);
                    }

                    checkCommands.execute().addListener(future -> future().setSuccess());
                }
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "checkSentinelsSlaves";
            }
        };
    }

    Command<Void> getSentinelsSlaves(Set<SentinelHello> hellos) {
        return new AbstractCommand<Void>() {
            @Override
            protected void doExecute() {
                ParallelCommandChain sentinelSlavesCommands = new ParallelCommandChain(resetExecutor, false);
                for (SentinelHello hello : hellos) {
                    HostPort sentinelAddr = hello.getSentinelAddr();
                    Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());
                    Command<List<HostPort>> slavesCommand = sentinelManager.slaves(sentinel, context.getSentinelMonitorName());
                    slavesCommand.future().addListener(future -> {
                        if (future.isSuccess())
                            allSentinels.put(sentinelAddr, future.get());
                        else {
                            logger.warn("[{}-{}][reset]sentinel slaves failed: {}", LOG_TITLE, context.getSentinelMonitorName(), sentinel, future.cause());
                        }
                    });
                    sentinelSlavesCommands.add(slavesCommand);
                }
                sentinelSlavesCommands.execute().addListener(future -> future().setSuccess());
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "getSentinelsSlaves";
            }
        };
    }

    List<HostPort> sentinelsToReset() {
        List<HostPort> shouldResetSentinels = Lists.newArrayList(this.shouldResetSentinels.keySet());

        List<HostPort> sentinelsToReset = new ArrayList<>();
        if (shouldResetSentinels.isEmpty())
            return sentinelsToReset;

        //reset unavailable sentinels first
        sentinelsToReset.addAll(shouldResetSentinels.stream().filter(sentinel -> !availableSentinels.containsKey(sentinel)).collect(Collectors.toList()));
        shouldResetSentinels.removeAll(sentinelsToReset);

        //all shouldResetSentinels unavailable
        if (shouldResetSentinels.isEmpty())
            return sentinelsToReset;

        //leave quorum availableSentinels
        int canResetAvailableSentinelSize = availableSentinels.size() - checkerConfig.getDefaultSentinelQuorumConfig().getQuorum();
        if (canResetAvailableSentinelSize >= shouldResetSentinels.size()) {
            sentinelsToReset.addAll(shouldResetSentinels);
            return sentinelsToReset;
        }

        for (int i = 0; i < canResetAvailableSentinelSize; i++) {
            sentinelsToReset.add(shouldResetSentinels.get(i));
        }

        return sentinelsToReset;
    }

    void filterAvailableSentinels() {
        allSentinels.forEach((sentinel, slaves) -> {
            if (sentinelHasAllSlaves(slaves)) {
                availableSentinels.put(sentinel, slaves);
            }
        });
    }

    boolean overHalfSentinelsLostSlaves() {
        return availableSentinels.size() < checkerConfig.getDefaultSentinelQuorumConfig().getQuorum();
    }

    private Set<HostPort> tooManyKeepers(List<HostPort> slaves) {
        if (!context.getInfo().getClusterType().supportKeeper())
            return new HashSet<>();

        Set<HostPort> allKeepers = metaCache.getAllKeepers();
        Set<HostPort> keepers = new HashSet<>();
        for (HostPort currentSlave : slaves) {
            if (allKeepers.contains(currentSlave)) {
                keepers.add(currentSlave);
            }
        }
        slaves.removeAll(keepers);
        if (keepers.size() > EXPECTED_KEEPER_COUNT) return keepers;
        return new HashSet<>();
    }


    private Set<HostPort> unknownInstances(List<HostPort> slaves) {
        Set<HostPort> unknownInstances = Sets.newHashSet(slaves);
        unknownInstances.removeAll(context.getShardInstances());
        slaves.removeAll(unknownInstances);
        return unknownInstances;
    }

    private static final int EXPECTED_KEEPER_COUNT = 1;
    private static final int EXPECTED_MASTER_COUNT = 1;

    boolean shouldReset(HostPort sentinelAddr) {
        SentinelInvalidSlaves invalidSlaves = problemSentinels.get(sentinelAddr);
        if (shouldResetTooManyKeepers(invalidSlaves.getTooManyKeepers())) {
            logger.info("[{}-{}][reset]{}, too many keepers: {}", LOG_TITLE, context.getSentinelMonitorName(), sentinelAddr, invalidSlaves.getTooManyKeepers());
            return true;
        }

        if (shouldResetUnknownInstances(invalidSlaves.getUnknownSlaves())) {
            logger.info("[{}-{}][reset]{}, unknown slaves: {}", LOG_TITLE, context.getSentinelMonitorName(), sentinelAddr, invalidSlaves.getUnknownSlaves());
            return true;
        }
        return false;
    }

    private boolean sentinelHasAllSlaves(List<HostPort> sentinelSlaves) {
        if (sentinelSlaves.isEmpty())
            return false;

        Set<HostPort> shardInstances = Sets.newHashSet(context.getShardInstances());
        shardInstances.removeAll(sentinelSlaves);
        return shardInstances.size() == EXPECTED_MASTER_COUNT;
    }

    private boolean shouldResetTooManyKeepers(Set<HostPort> toManyKeepers) {
        if (toManyKeepers.isEmpty())
            return false;

        Set<HostPort> slaves = Sets.newHashSet(masterSlaves);
        slaves.retainAll(toManyKeepers);

        return slaves.size() <= EXPECTED_KEEPER_COUNT;
    }

    private boolean shouldResetUnknownInstances(Set<HostPort> unknownSlaves) {
        if (unknownSlaves.isEmpty())
            return false;

        Set<HostPort> slaves = Sets.newHashSet(masterSlaves);
        slaves.retainAll(unknownSlaves);

        return slaves.isEmpty();
    }

    private boolean masterHasAllSlaves() {
        HostPort master = context.getTrueMasterInfo().getKey();
        Set<HostPort> masterAndSlaves = Sets.newHashSet(masterSlaves);
        masterAndSlaves.add(master);

        boolean masterHasAllSlaves = masterAndSlaves.containsAll(context.getShardInstances());
        if (!masterHasAllSlaves) {
            logger.warn("[{}-{}][reset]master:{} lost connection with some slaves, stop reset, current slaves:{}, expected slaves:{}", LOG_TITLE, context.getSentinelMonitorName(), master, masterSlaves, context.getShardInstances());
        }
        return masterHasAllSlaves;
    }

    class CheckSentinelSlaves extends AbstractCommand<SentinelInvalidSlaves> {

        private List<HostPort> slaves;

        public CheckSentinelSlaves(List<HostPort> slaves) {
            this.slaves = slaves;
        }

        @Override
        public String getName() {
            return "CheckSentinelSlaves";
        }

        @Override
        protected void doExecute() throws Throwable {
            if (slaves.isEmpty())
                future().setSuccess(new SentinelInvalidSlaves());
            else {
                Set<HostPort> toManyKeepers = tooManyKeepers(slaves);
                Set<HostPort> unknownSlaves = unknownInstances(slaves);
                future().setSuccess(new SentinelInvalidSlaves(toManyKeepers, unknownSlaves));
            }
        }

        @Override
        protected void doReset() {

        }
    }

    ResetSentinels setKeyedObjectPool(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
        this.keyedObjectPool = keyedObjectPool;
        return this;
    }

    ResetSentinels setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }

}
