package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.observer.Event;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.observer.NodeModified;
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

    private static final String PEER_CHANGE_TYPE = "crdt.peerchange";

    private static final String TEMP_PEER_CHANGE = "[%s][%s][%s:%d][%s] peermaster %s, gid: %d";

    private static final String PEER_SKIP_DELETE = "skipdelete";

    private static final String PEER_DELETE = "delete";

    private static final String PEER_ADD = "add";

    private static final String PEER_CHANGE= "change";

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

    private Map<Long, NodeDeleted<HostPort>> peerMasterDeleted;

    private Map<Long, NodeAdded<HostPort>> peerMasterAdded;

    private Map<Long, NodeModified<HostPort>> peerMasterChanged;

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
            } else if (peerMasterChanged.isEmpty() && peerMasterAdded.isEmpty() && (!doDelete || peerMasterDeleted.isEmpty())) {
                getLogger().debug("[doExecute] no need adjust, finish");
                future().setSuccess();
            } else {
                doPeerMasterAdjust();
            }
        });

        sequenceCommandChain.execute(executors);
    }

    private void doPeerMasterAdjust() {
        SequenceCommandChain sequenceCommandChain = new SequenceCommandChain(true);
        peerMasterChanged.forEach((gid, nodeModified) -> {
            HostPort peerMaster = nodeModified.getNewNode();
            logOperation(PEER_CHANGE, currentMaster, gid, nodeModified);
            sequenceCommandChain.add(retryCommandWrap(new PeerOfCommand(clientPool, gid, peerMaster.getHost(), peerMaster.getPort(), scheduled)));
        });
        peerMasterAdded.forEach((gid, nodeAdded) -> {
            HostPort peerMaster = nodeAdded.getNode();
            logOperation(PEER_ADD, currentMaster, gid, nodeAdded);
            sequenceCommandChain.add(retryCommandWrap(new PeerOfCommand(clientPool, gid, peerMaster.getHost(), peerMaster.getPort(), scheduled)));
        });
        peerMasterDeleted.forEach((gid, nodeDeleted) -> {
            if (doDelete) {
                logOperation(PEER_DELETE, currentMaster, gid, nodeDeleted);
                sequenceCommandChain.add(retryCommandWrap(new PeerOfCommand(clientPool, gid, scheduled)));
            } else {
                logOperation(PEER_SKIP_DELETE, currentMaster, gid, nodeDeleted);
            }
        });
        sequenceCommandChain.future().addListener(commandFuture -> {
            if (commandFuture.isSuccess()) future().setSuccess();
            else future().setFailure(commandFuture.cause());
        });

        sequenceCommandChain.execute(executors);
    }

    private void logOperation(String type, Pair<String, Integer> currentMaster, long gid, Event event) {
        String logContent = String.format(TEMP_PEER_CHANGE, clusterId, shardId, currentMaster.getKey(), currentMaster.getValue(), type, event, gid);
        getLogger().info(logContent);
        CatEventMonitor.DEFAULT.logEvent(PEER_CHANGE_TYPE, logContent);
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
            Map<Long, HostPort> flatExpectedPeerMaster = parsePeerMasters(upstreamPeerMasters);
            Map<Long, HostPort> flatCurrentPeerMaster = parsePeerMasters(currentPeerMasters);
            Set<Long> retainGidSet = new HashSet<>(flatExpectedPeerMaster.keySet());
            Set<Long> currentGidSet = flatCurrentPeerMaster.keySet();
            retainGidSet.retainAll(currentGidSet);

            makePeerMasterChange(flatExpectedPeerMaster, flatCurrentPeerMaster, retainGidSet);
            makePeerMasterAdded(flatExpectedPeerMaster, retainGidSet);
            makePeerMasterDeleted(flatCurrentPeerMaster, retainGidSet);

            future().setSuccess();
        }

        private Map<Long, HostPort> parsePeerMasters(List<RedisMeta> peerMasters) {
            Map<Long, HostPort> flatPeerMasterInfo = new HashMap<>();
            peerMasters.forEach(peerMaster -> flatPeerMasterInfo.put(peerMaster.getGid(), new HostPort(peerMaster.getIp(), peerMaster.getPort())));
            return flatPeerMasterInfo;
        }

        private void makePeerMasterChange(Map<Long, HostPort> expectedPeerMaster, Map<Long, HostPort> currentPeerMaster, Set<Long> retainGid) {
            peerMasterChanged = new HashMap<>();

            retainGid.forEach(gid -> {
                if (!expectedPeerMaster.get(gid).equals(currentPeerMaster.get(gid))) {
                    peerMasterChanged.put(gid, new NodeModified<>(currentPeerMaster.get(gid), expectedPeerMaster.get(gid)));
                }
            });
        }

        private void makePeerMasterAdded(Map<Long, HostPort> expectedPeerMaster, Set<Long> retainGid) {
            peerMasterAdded = new HashMap<>();
            expectedPeerMaster.forEach((gid, peerMaster) -> {
                if (!retainGid.contains(gid)) {
                    peerMasterAdded.put(gid, new NodeAdded<>(peerMaster));
                }
            });
        }

        private void makePeerMasterDeleted(Map<Long, HostPort> currentPeerMaster, Set<Long> retainGid) {
            peerMasterDeleted = new HashMap<>();

            currentPeerMaster.forEach((gid, peerMaster) -> {
                if (!retainGid.contains(gid)) {
                    peerMasterDeleted.put(gid, new NodeDeleted<>(peerMaster));
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
