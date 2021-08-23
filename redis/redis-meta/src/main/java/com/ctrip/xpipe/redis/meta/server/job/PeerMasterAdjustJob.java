package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Event;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.observer.NodeModified;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.cmd.*;
import com.ctrip.xpipe.redis.meta.server.exception.BadRedisVersionException;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public class PeerMasterAdjustJob extends AbstractCommand<Void> {

    private static final String PEER_CHANGE_TYPE = "crdt.peerchange";

    private static final String TEMP_PEER_CHANGE = "[%s][%s][%s:%d][%s] peermaster %s, gid: %d";

    private static final String PEER_SKIP_DELETE = "skipdelete";

    private static final String PEER_DELETE = "delete";

    private static final String PEER_ADD = "add";

    private static final String PEER_CHANGE= "change";

    private static final String CRDT_REDIS_VERSION_KEY = "xredis_crdt_version";

    private static final String MINIMUM_SUPPORTED_VERSION = "1.0.4";

    private String clusterId;

    private String shardId;

    //gid + endpoint
    private List<Pair<Long,Endpoint>> upstreamPeerMasters;

    private Pair<String, Integer> currentMaster;

    private List<Pair<Long,Endpoint>> currentPeerMasters;

    private SimpleObjectPool<NettyClient> clientPool;

    private int delayBaseMilli = 1000;

    private int retryTimes = 5;

    private ScheduledExecutorService scheduled;

    private Executor executors;

    private Map<Long, NodeDeleted<Endpoint>> peerMasterDeleted;

    private Map<Long, NodeAdded<Endpoint>> peerMasterAdded;

    private Map<Long, NodeModified<Endpoint>> peerMasterChanged;

    private boolean doDelete = true;

    public PeerMasterAdjustJob(String clusterId, String shardId, List<Pair<Long,Endpoint>> upstreamPeerMasters,
                               Pair<String, Integer> currentMaster, boolean doDelete,
                               SimpleObjectPool<NettyClient> clientPool
            , ScheduledExecutorService scheduled, Executor executors) {
        this(clusterId, shardId, upstreamPeerMasters, currentMaster, doDelete, clientPool, 1000, 5, scheduled, executors);
    }

    public PeerMasterAdjustJob(String clusterId, String shardId, List<Pair<Long,Endpoint>> upstreamPeerMasters,
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
        sequenceCommandChain.add(retryCommandWrap(new MasterVersionCheckCommand()));
        sequenceCommandChain.add(retryCommandWrap(new GetCurrentMasterCommand()));
        sequenceCommandChain.add(new PeerMasterCompareCommand());

        sequenceCommandChain.future().addListener(commandFuture -> {
            if (!commandFuture.isSuccess()) {
                getLogger().info("[{}][{}] fail", clusterId, shardId, commandFuture.cause());
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
            Endpoint peerMaster = nodeModified.getNewNode();
            logOperation(PEER_CHANGE, currentMaster, gid, nodeModified);
            sequenceCommandChain.add(retryCommandWrap(new PeerOfCommand(clientPool, gid, peerMaster, scheduled)));
        });
        peerMasterAdded.forEach((gid, nodeAdded) -> {
            Endpoint peerMaster = nodeAdded.getNode();
            logOperation(PEER_ADD, currentMaster, gid, nodeAdded);
            sequenceCommandChain.add(retryCommandWrap(new PeerOfCommand(clientPool, gid, peerMaster, scheduled)));
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
                StringUtil.join(",", (peerMaster) -> String.format("%d %s", peerMaster.getKey(), peerMaster.getValue()), upstreamPeerMasters),
                currentMaster);
    }

    private Command<?> retryCommandWrap(Command<?> command) {
        return CommandRetryWrapper.buildCountRetry(retryTimes, new RetryDelay(delayBaseMilli) {

            @Override
            public boolean retry(Throwable th) {
                if (th instanceof BadRedisVersionException) return false;
                return true;
            }

        }, command, scheduled);
    }

    class MasterVersionCheckCommand extends AbstractCommand<Void> {
        @Override
        protected void doExecute() throws InterruptedException, ExecutionException, BadRedisVersionException {
            InfoCommand infoCommand = new InfoCommand(clientPool, InfoCommand.INFO_TYPE.SERVER.cmd(), scheduled, delayBaseMilli);
            infoCommand.future().addListener(new CommandFutureListener<String>() {
                @Override
                public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                    if (commandFuture.isSuccess()) {
                        handleResult(commandFuture.get());
                    } else {
                        future().setFailure(commandFuture.cause());
                    }
                }
            });

            infoCommand.execute(executors);
        }

        protected void handleResult(String rawInfo) {
            try {
                InfoResultExtractor extractor = new InfoResultExtractor(rawInfo);
                String masterVersion = extractor.extract(CRDT_REDIS_VERSION_KEY);

                if (StringUtil.isEmpty(masterVersion)) {
                    future().setFailure(new BadRedisVersionException(String.format("master %s:%d is not crdt redis",
                            currentMaster.getKey(), currentMaster.getValue())));
                    return;
                }
                boolean isSupported;
                try {
                    isSupported = StringUtil.compareVersion(masterVersion, MINIMUM_SUPPORTED_VERSION) >= 0;
                } catch (NumberFormatException e) {
                    isSupported = masterVersion.compareTo(MINIMUM_SUPPORTED_VERSION) >= 0;
                }
                if (isSupported) {
                    future().setSuccess();
                } else {
                    future().setFailure(new RuntimeException(String.format("CRDT Redis version(%s) is too low", masterVersion)));
                }
                
            } catch (Exception e) {
                getLogger().info("[handleResult] parse master version fail", e);
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }
    }

    class GetCurrentMasterCommand extends AbstractCommand<Void> {
        @Override
        protected void doExecute() throws Exception {
            CRDTInfoCommand crdtInfoCommand = new CRDTInfoCommand(clientPool, InfoCommand.INFO_TYPE.REPLICATION.cmd(), scheduled, delayBaseMilli);
            crdtInfoCommand.future().addListener(new CommandFutureListener<String>() {
                @Override
                public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                    if (commandFuture.isSuccess()) {
                        String rawInfo = commandFuture.get();
                        handleResult(rawInfo);
                    } else {
                        future().setFailure(commandFuture.cause());
                    }
                }
            });

            crdtInfoCommand.execute(executors);
        }

        protected void handleResult(String rawInfo) {
            try {
                CRDTInfoResultExtractor extractor = new CRDTInfoResultExtractor(rawInfo);
                currentPeerMasters = extractor.extractPeerMasters();
                future().setSuccess();
            } catch (Exception e) {
                getLogger().info("[handleResult] parse current peermaster fail", e);
                future().setFailure(e);
            }
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
            Map<Long, Endpoint> flatExpectedPeerMaster = parsePeerMasters(upstreamPeerMasters);
            Map<Long, Endpoint> flatCurrentPeerMaster = parsePeerMasters(currentPeerMasters);
            Set<Long> retainGidSet = new HashSet<>(flatExpectedPeerMaster.keySet());
            Set<Long> currentGidSet = flatCurrentPeerMaster.keySet();
            retainGidSet.retainAll(currentGidSet);

            makePeerMasterChange(flatExpectedPeerMaster, flatCurrentPeerMaster, retainGidSet);
            makePeerMasterAdded(flatExpectedPeerMaster, retainGidSet);
            makePeerMasterDeleted(flatCurrentPeerMaster, retainGidSet);

            future().setSuccess();
        }

        private Map<Long, Endpoint> parsePeerMasters(List<Pair<Long, Endpoint>> peerMasters) {
            Map<Long, Endpoint> flatPeerMasterInfo = new HashMap<>();
            peerMasters.forEach(peerMaster -> flatPeerMasterInfo.put(peerMaster.getKey(), peerMaster.getValue()));
            return flatPeerMasterInfo;
        }

        private void makePeerMasterChange(Map<Long, Endpoint> expectedPeerMaster, Map<Long,  Endpoint> currentPeerMaster, Set<Long> retainGid) {
            peerMasterChanged = new HashMap<>();

            retainGid.forEach(gid -> {
                if (!expectedPeerMaster.get(gid).equals(currentPeerMaster.get(gid))) {
                    peerMasterChanged.put(gid, new NodeModified<>(currentPeerMaster.get(gid), expectedPeerMaster.get(gid)));
                }
            });
        }

        private void makePeerMasterAdded(Map<Long, Endpoint> expectedPeerMaster, Set<Long> retainGid) {
            peerMasterAdded = new HashMap<>();
            expectedPeerMaster.forEach((gid, peerMaster) -> {
                if (!retainGid.contains(gid)) {
                    peerMasterAdded.put(gid, new NodeAdded<>(peerMaster));
                }
            });
        }

        private void makePeerMasterDeleted(Map<Long, Endpoint> currentPeerMaster, Set<Long> retainGid) {
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
