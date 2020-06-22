package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.PeerOfCommand;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public class PeerMasterAdjustJob extends AbstractCommand<Void> {

    private static final String PEER_MASTER_TYPE = "peermaster";

    private static final String TEMP_SKIP_DELETE = "[%s][%s][skipdelete] master %s:%d has unknow peer master %d %s:%d";

    private static final String TEMP_DELETE = "[%s][%s][delete] master %s:%d delete unknow peer master %d %s:%d";

    private static final String TEMP_ADD = "[%s][%s][add] master %s:%d add peer master %d %s:%d";

    private static final String TEMP_CHANGE= "[%s][%s][change] master %s:%d has change peer master %d %s:%d";

    private String clusterId;

    private String shardId;

    private List<RedisMeta> upstreamPeerMasters;

    private Pair<String, Integer> currentMaster;

    private List<RedisMeta> currentPeerMasters;

    private SimpleObjectPool<NettyClient> clientPool;

    private int delayBaseMilli = 1000;

    private int retryTimes = 5;

    private ScheduledExecutorService scheduled;

    private Executor executors;

    private Map<Long, Pair<String, Integer> > peerMasterDeleted;

    private Map<Long, Pair<String, Integer> > peerMasterAdded;

    private Map<Long, Pair<String, Integer> > peerMasterChanged;

    private boolean doDelete = true;

    public PeerMasterAdjustJob(String clusterId, String shardId, List<RedisMeta> upstreamPeerMasters,
                               Pair<String, Integer> currentMaster, boolean doDelete,
                               SimpleObjectPool<NettyClient> clientPool
            , ScheduledExecutorService scheduled, Executor executors) {
        this(clusterId, shardId, upstreamPeerMasters, currentMaster, doDelete, clientPool, 1000, 5, scheduled, executors);
    }

    public PeerMasterAdjustJob(String clusterId, String shardId, List<RedisMeta> upstreamPeerMasters,
                               Pair<String, Integer> currentMaster, boolean doDelete,
                               SimpleObjectPool<NettyClient> clientPool
            , int delayBaseMilli, int retryTimes, ScheduledExecutorService scheduled, Executor executors) {
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.upstreamPeerMasters = new LinkedList<>(upstreamPeerMasters);
        this.currentMaster = currentMaster;
        this.clientPool = clientPool;
        this.delayBaseMilli = delayBaseMilli;
        this.retryTimes = retryTimes;
        this.scheduled = scheduled;
        this.executors = executors;
        this.doDelete = doDelete;
    }

    @Override
    public String getName() {
        return "peer master adjust job";
    }

    @Override
    protected void doExecute() throws CommandExecutionException {
        SequenceCommandChain sequenceCommandChain = new SequenceCommandChain();
        sequenceCommandChain.add(retryCommandWrap(new GetCurrentMasterCommand()));
        sequenceCommandChain.add(retryCommandWrap(new PeerMasterCompareCommand()));

        sequenceCommandChain.future().addListener(commandFuture -> {
            if (!commandFuture.isSuccess()) {
                future().setFailure(commandFuture.cause());
            } else {
                doPeerMasterAdjust();
            }
        });

        sequenceCommandChain.execute(executors);
    }

    private void doPeerMasterAdjust() {
        SequenceCommandChain sequenceCommandChain = new SequenceCommandChain(true);
        peerMasterChanged.forEach((gid, peerMaster) -> {
            logOperation(TEMP_CHANGE, currentMaster, gid, peerMaster);
            sequenceCommandChain.add(retryCommandWrap(new PeerOfCommand(clientPool, gid, peerMaster.getKey(), peerMaster.getValue(), scheduled)));
        });
        peerMasterAdded.forEach((gid, peerMaster) -> {
            logOperation(TEMP_ADD, currentMaster, gid, peerMaster);
            sequenceCommandChain.add(retryCommandWrap(new PeerOfCommand(clientPool, gid, peerMaster.getKey(), peerMaster.getValue(), scheduled)));
        });
        peerMasterDeleted.forEach((gid, peerMaster) -> {
            if (doDelete) {
                logOperation(TEMP_DELETE, currentMaster, gid, peerMaster);
                sequenceCommandChain.add(retryCommandWrap(new PeerOfCommand(clientPool, gid, scheduled)));
            } else {
                logOperation(TEMP_SKIP_DELETE, currentMaster, gid, peerMaster);
            }
        });
        sequenceCommandChain.future().addListener(commandFuture -> {
            if (commandFuture.isSuccess()) future().setSuccess();
            else future().setFailure(commandFuture.cause());
        });

        sequenceCommandChain.execute(executors);
    }

    private void logOperation(String logTemp, Pair<String, Integer> currentMaster, long gid, Pair<String, Integer> peerMaster) {
        String logContent = String.format(logTemp, clusterId, shardId, currentMaster.getKey(), currentMaster.getValue(), gid, peerMaster.getKey(), peerMaster.getValue());
        getLogger().info("[PeerMasterAdjustJob]{}", logContent);
        CatEventMonitor.DEFAULT.logEvent(PEER_MASTER_TYPE, logContent);
    }

    @Override
    protected void doReset(){
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return String.format("[%s] master: %s",
                StringUtil.join(",", (peerMaster) -> String.format("%d %s:%d", peerMaster.getGid(), peerMaster.getIp(), peerMaster.getPort()), upstreamPeerMasters),
                currentMaster);
    }

    private Command<?> retryCommandWrap(Command<?> command) {
        return CommandRetryWrapper.buildCountRetry(retryTimes, new RetryDelay(delayBaseMilli), command, scheduled);
    }

    class GetCurrentMasterCommand extends AbstractCommand<Void> {
        @Override
        protected void doExecute() throws Exception {
            CRDTInfoCommand crdtInfoCommand = new CRDTInfoCommand(clientPool, InfoCommand.INFO_TYPE.REPLICATION.cmd(), scheduled, delayBaseMilli);
            String rawInfo = crdtInfoCommand.execute().get();
            CRDTInfoResultExtractor extractor = new CRDTInfoResultExtractor(rawInfo);
            currentPeerMasters = extractor.extractPeerMasters();
            future().setSuccess();
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }
    }

    class PeerMasterCompareCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            Map<Long, Pair<String, Integer> > flatExpectedPeerMaster = parsePeerMasters(upstreamPeerMasters);
            Map<Long, Pair<String, Integer> > flatCurrentPeerMaster = parsePeerMasters(currentPeerMasters);
            Set<Long> retainGidSet = new HashSet<>(flatExpectedPeerMaster.keySet());
            Set<Long> currentGidSet = flatCurrentPeerMaster.keySet();
            retainGidSet.retainAll(currentGidSet);

            makePeerMasterChange(flatExpectedPeerMaster, flatCurrentPeerMaster, retainGidSet);
            makePeerMasterAdded(flatExpectedPeerMaster, retainGidSet);
            makePeerMasterDeleted(flatCurrentPeerMaster, retainGidSet);

            future().setSuccess();
        }

        private Map<Long, Pair<String, Integer> > parsePeerMasters(List<RedisMeta> peerMasters) {
            Map<Long, Pair<String, Integer> > flatPeerMasterInfo = new HashMap<>();
            peerMasters.forEach(peerMaster -> flatPeerMasterInfo.put(peerMaster.getGid(), Pair.of(peerMaster.getIp(), peerMaster.getPort())));
            return flatPeerMasterInfo;
        }

        private void makePeerMasterChange(Map<Long, Pair<String, Integer> > expectedPeerMaster, Map<Long, Pair<String, Integer> > currentPeerMaster, Set<Long> retainGid) {
            peerMasterChanged = new HashMap<>();

            retainGid.forEach(gid -> {
                if (!expectedPeerMaster.get(gid).equals(currentPeerMaster.get(gid))) {
                    peerMasterChanged.put(gid, expectedPeerMaster.get(gid));
                }
            });
        }

        private void makePeerMasterAdded(Map<Long, Pair<String, Integer> > expectedPeerMaster, Set<Long> retainGid) {
            peerMasterAdded = new HashMap<>();
            expectedPeerMaster.forEach((gid, peerMaster) -> {
                if (!retainGid.contains(gid)) {
                    peerMasterAdded.put(gid, peerMaster);
                }
            });
        }

        private void makePeerMasterDeleted(Map<Long, Pair<String, Integer> > currentPeerMaster, Set<Long> retainGid) {
            peerMasterDeleted = new HashMap<>();

            currentPeerMaster.forEach((gid, peerMaster) -> {
                if (!retainGid.contains(gid)) {
                    peerMasterDeleted.put(gid, peerMaster);
                }
            });
        }

        @Override
        protected void doReset() {
        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

    }

}
