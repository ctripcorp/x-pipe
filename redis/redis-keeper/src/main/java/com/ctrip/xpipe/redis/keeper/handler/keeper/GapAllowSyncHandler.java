package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.error.NoMasterlinkRedisError;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.store.BacklogOffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.XSyncContinue;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;

import java.io.IOException;
import java.util.Collections;

import static com.ctrip.xpipe.redis.core.protocal.Psync.PARTIAL_SYNC;

public abstract class GapAllowSyncHandler extends AbstractCommandHandler {

    @Override
    protected void doHandle(final String[] args, final RedisClient<?> redisClient) throws Exception {
        final RedisKeeperServer redisKeeperServer = (RedisKeeperServer) redisClient.getRedisServer();

        if(redisKeeperServer.rdbDumper() == null && redisKeeperServer.getReplicationStore().isFresh()){
            redisClient.sendMessage(new RedisErrorParser(new NoMasterlinkRedisError("Can't SYNC while replicationstore fresh")).format());
            return;
        }

        if(!redisKeeperServer.getRedisKeeperServerState().psync(redisClient, args)){
            return;
        }

        final RedisSlave redisSlave  = redisClient.becomeGapAllowRedisSlave();
        if(redisSlave == null){
            logger.warn("[doHandle][client already slave] {}", redisClient);
            try {
                redisClient.close();
            } catch (IOException e) {
                logger.error("[doHandle]" + redisClient, e);
            }
            return;
        }

        // transfer to psync executor, which will do psync dedicatedly
        redisSlave.processPsyncSequentially(new Runnable() {
            @Override
            public void run() {
                try {
                    SyncRequest request = parseRequest(args, redisSlave);
                    if (null == request) {
                        logger.warn("[doHandle] request parse fail");
                        return;
                    }

                    SyncAction action = anaRequest(request, redisKeeperServer, redisSlave);
                    if (null == action) {
                        logger.warn("[doHandle] request analyze fail");
                        return;
                    }

                    runAction(action, redisKeeperServer, redisSlave);
                } catch(Throwable th) {
                    try {
                        logger.error("[run]" + redisClient, th);
                        if(redisSlave.isOpen()){
                            redisSlave.close();
                        }
                    } catch (IOException e) {
                        logger.error("[run][close]" + redisSlave, th);
                    }
                }
            }
        });
    }

    protected abstract SyncRequest parseRequest(final String[] args, RedisSlave redisSlave);

    protected SyncAction anaRequest(SyncRequest request, RedisKeeperServer redisKeeperServer, RedisSlave slave) throws Exception {
        KeeperRepl keeperRepl = redisKeeperServer.getKeeperRepl();
        KeeperConfig keeperConfig = redisKeeperServer.getKeeperConfig();

        ReplStage preStage = keeperRepl.preStage();
        ReplStage curStage = keeperRepl.currentStage();

        if (null == preStage && null == curStage) {
            slave.sendMessage(new RedisErrorParser(new NoMasterlinkRedisError("Can't SYNC while replicationstore fresh")).format());
            return null;
        }

        if (request.replId.equals("?")) {
            if (request.offset == -2) {
                SyncAction action;
                if (curStage.getProto() == ReplStage.ReplProto.PSYNC) {
                    long offset = keeperRepl.getEndOffset() + 1;
                    action = SyncAction.Continue(curStage, curStage.getReplId(), offset).markKeeperPartial();
                } else {
                    XSyncContinue xsyncCont = redisKeeperServer.locateContinueGtidSet(keeperRepl.getEndGtidSet());
                    action = SyncAction.XContinue(curStage, xsyncCont.getContinueGtidSet().union(curStage.getGtidLost()),
                            xsyncCont.getBacklogOffset(), null).markKeeperPartial().setKeeperLost(curStage.getGtidLost());
                }
                return action;
            } else if (request.offset == -3) {
                return SyncAction.full("req fresh rdb fsync", true);
            } else {
                return SyncAction.full("req fsync");
            }
        }

        if (null != curStage && curStage.getProto() == request.proto) {
            if (request.proto == ReplStage.ReplProto.PSYNC) {
                return anaPSync(request, curStage, keeperRepl, keeperConfig, -1);
            } else {
                XSyncContinue xsyncCont = null;
                xsyncCont = redisKeeperServer.locateContinueGtidSet(request.slaveGtidSet);
                return anaXSync(request, curStage, xsyncCont, keeperRepl, keeperConfig, -1);
            }
        } else if (null != curStage && null != preStage && preStage.getProto() == request.proto) {
            logger.info("[anaRequest][usePreReplStage][pre:{}][cur:{}]", preStage, curStage);
            long reqBacklogOffset = -1;
            XSyncContinue xsyncCont = null;
            if (request.proto == ReplStage.ReplProto.PSYNC) {
                reqBacklogOffset = preStage.replOffset2BacklogOffset(request.offset);
            } else {
                xsyncCont = redisKeeperServer.locateContinueGtidSet(request.slaveGtidSet);
                reqBacklogOffset = xsyncCont.getBacklogOffset();
            }

            if (-1 == reqBacklogOffset) {
                return SyncAction.full("locate backlog offset fail");
            }

            if (reqBacklogOffset == curStage.getBegOffsetBacklog()) {
                return switchProto(curStage);
            }

            if (request.proto == ReplStage.ReplProto.PSYNC) {
                return anaPSync(request, preStage, keeperRepl, keeperConfig, curStage.getBegOffsetBacklog());
            } else {
                return anaXSync(request, preStage, xsyncCont, keeperRepl, keeperConfig, curStage.getBegOffsetBacklog());
            }
        }

        return SyncAction.full("no repl stage match");
    }

    private SyncAction switchProto(ReplStage replStage) {
        if (replStage.getProto() == ReplStage.ReplProto.PSYNC) {
            return SyncAction.Continue(replStage, replStage.getReplId(), replStage.getBegOffsetRepl()).markProtoSwitch();
        } else {
            // TODO: sync gtid.lost with slave
            return SyncAction.XContinue(replStage, replStage.getBeginGtidset(), replStage.getBegOffsetBacklog(), null).markProtoSwitch();
        }
    }

    protected SyncAction anaPSync(SyncRequest request, ReplStage psyncStage, KeeperRepl keeperRepl, KeeperConfig config, long stageEndBacklogOffsetExcluded) {
        if (request.offset < psyncStage.getBegOffsetRepl()) {
            return SyncAction.full(String.format("[request offset miss][repl][req: %d, sup:%d]", request.offset, psyncStage.getBegOffsetRepl()));
        }

        long reqBacklogOffset = psyncStage.replOffset2BacklogOffset(request.offset);
        String keeperReplId = psyncStage.getReplId();
        String keeperReplId2 = psyncStage.getReplId2();
        long keeperOffset2 = psyncStage.getSecondReplIdOffset();
        long backlogBeginOffset = Math.max(psyncStage.getBegOffsetBacklog(),keeperRepl.backlogBeginOffset());
        long backlogEnd = keeperRepl.backlogEndOffset();
        long maxTransfer = config.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb();

        if (request.replId.equalsIgnoreCase(keeperReplId) || (request.replId.equalsIgnoreCase(keeperReplId2) && request.offset <= keeperOffset2)) {
            if (reqBacklogOffset < backlogBeginOffset) {
                return SyncAction.full(String.format("[request offset miss][backlog][req: %d, sup:%d]", reqBacklogOffset, backlogBeginOffset));
            } else if (backlogEnd - reqBacklogOffset >= maxTransfer) {
                return SyncAction.full(String.format("[too much commands to transfer]%d - %d < %d", backlogEnd, reqBacklogOffset, maxTransfer));
            } else {
                return SyncAction.Continue(psyncStage, keeperReplId, request.offset).setBacklogEndExcluded(stageEndBacklogOffsetExcluded);
            }
        } else {
            return SyncAction.full("replId mismatch");
        }
    }

    protected SyncAction anaXSync(SyncRequest request, ReplStage xsyncStage, XSyncContinue xsyncCont, KeeperRepl keeperRepl, KeeperConfig config, long stageEndBacklogOffset) {
        GtidSet lost = xsyncStage.getGtidLost();
        GtidSet cont = xsyncCont.getContinueGtidSet();
        GtidSet req = request.slaveGtidSet;
        long backlogCont = xsyncCont.getBacklogOffset();
        long backlogEnd = keeperRepl.backlogEndOffset();
        long maxTransfer = config.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb();

        if ("*".equals(request.replId) || xsyncStage.getReplId().equalsIgnoreCase(request.replId)) {
            GtidSet masterGtidSet = cont.union(lost);
            GtidSet gap = masterGtidSet.symmetricDiff(req);
            GtidSet masterLost = req.subtract(masterGtidSet);
            int gapCnt = gap.itemCnt();
            // TODO: keeper wait transform limit
            if (request.maxGap >= 0 && request.maxGap < gapCnt) {
                return SyncAction.full(String.format("[gap][%d > %d]", gapCnt, request.maxGap));
            } else if (backlogEnd - backlogCont >= maxTransfer) {
                return SyncAction.full(String.format("[too much commands to transfer]%d - %d < %d", backlogEnd, backlogCont, maxTransfer));
            } else {
                return SyncAction.XContinue(xsyncStage, masterGtidSet, backlogCont, masterLost).setBacklogEndExcluded(stageEndBacklogOffset);
            }
        } else {
            return SyncAction.full("replId mismatch");
        }
    }

    protected void runAction(SyncAction action, RedisKeeperServer keeperServer, RedisSlave slave) {
        if (action.isFull()) {
            try {
                logger.info("[runAction][full] {}", action.fullCause);
                slave.markPsyncProcessed();
                keeperServer.fullSyncToSlave(slave, action.freshRdb);
                keeperServer.getKeeperMonitor().getKeeperStats().increaseFullSync();
            } catch (IOException ioException) {
                try {
                    slave.close();
                } catch (Throwable th) {
                    logger.info("[runAction][close fail]");
                }
            }
        } else {
            if (null != action.masterLost && !action.masterLost.isEmpty()) {
                try {
                    keeperServer.increaseLost(action.masterLost, slave);
                } catch (IOException e) {
                    try {
                        slave.close();
                        return;
                    } catch (Throwable th) {
                        logger.info("[runAction][close fail]");
                    }
                }
            }

            String respStr;
            if (action.replStage.getProto() == ReplStage.ReplProto.PSYNC) {
                respStr = String.format("%s %s", PARTIAL_SYNC, action.replId)
                        + (action.protoSwitch || action.keeperPartial ? " "+(action.replOffset - 1):"");
            } else {
                respStr = String.format("XCONTINUE GTID.SET %s MASTER.UUID %s REPLID %s REPLOFF %d",
                        action.gtidSet.toString(), action.replStage.getMasterUuid(), action.replId, action.replOffset - 1)
                        + (action.keeperPartial ? " GTID.LOST " + action.keeperLost:"");
            }

            logger.info("[runAction][resp] {}", respStr);
            slave.sendMessage(new SimpleStringParser(respStr).format());
            slave.markPsyncProcessed();
            slave.beginWriteCommands(new BacklogOffsetReplicationProgress(action.backlogOffset, action.backlogEndOffsetExcluded));
            slave.partialSync();
            keeperServer.getKeeperMonitor().getKeeperStats().increaseFullSync();
        }
    }

    protected static class SyncAction {

        boolean full = false;

        boolean freshRdb = false;

        boolean keeperPartial = false;

        String fullCause;

        boolean protoSwitch = false;

        String replId;

        long replOffset = -1;

        long backlogOffset = -1;

        long backlogEndOffsetExcluded = -1;

        ReplStage replStage;

        GtidSet gtidSet;

        GtidSet masterLost;

        GtidSet keeperLost;

        public static SyncAction full(String fullCause) {
            return full(fullCause, false);
        }

        public static SyncAction full(String fullCause, boolean freshRdb) {
            SyncAction action = new SyncAction();
            action.full = true;
            action.freshRdb = freshRdb;
            action.fullCause = fullCause;
            return action;
        }

        public static SyncAction XContinue(ReplStage replStage, GtidSet contGtidSet, long backlogOffset, GtidSet masterLost) {
            SyncAction action = new SyncAction();
            action.replStage = replStage;
            action.gtidSet = contGtidSet;
            action.replId = replStage.getReplId();
            action.replOffset = replStage.backlogOffset2ReplOffset(backlogOffset);
            action.backlogOffset = backlogOffset;
            action.masterLost = masterLost;
            return action;
        }

        public static SyncAction Continue(ReplStage replStage, String replId, long offset) {
            SyncAction action = new SyncAction();
            action.replStage = replStage;
            action.replId = replId;
            action.replOffset = offset;
            action.backlogOffset = replStage.replOffset2BacklogOffset(offset);
            return action;
        }

        public boolean isFull() {
            return full;
        }

        public String getFullCause() {
            return fullCause;
        }

        public String getReplId() {
            return replId;
        }

        public long getReplOffset() {
            return replOffset;
        }

        public ReplStage getReplStage() {
            return replStage;
        }

        public GtidSet getGtidSet() {
            return gtidSet;
        }

        public SyncAction markProtoSwitch() {
            this.protoSwitch = true;
            return this;
        }

        public SyncAction markKeeperPartial() {
            this.keeperPartial = true;
            return this;
        }

        public SyncAction setBacklogEndExcluded(long backlogEndOffsetExcluded) {
            this.backlogEndOffsetExcluded = backlogEndOffsetExcluded;
            return this;
        }

        public SyncAction setKeeperLost(GtidSet lost) {
            this.keeperLost = lost;
            return this;
        }

    }

    protected static class SyncRequest {

        public ReplStage.ReplProto proto;

        public String replId;

        public long offset = -1;

        public GtidSet slaveGtidSet;

        public long maxGap = -1;

        private SyncRequest() {

        }

        public static SyncRequest psync(String replId, long offset) {
            SyncRequest request = new SyncRequest();
            request.proto = ReplStage.ReplProto.PSYNC;
            request.replId = replId;
            request.offset = offset;
            return request;
        }

        public static SyncRequest xsync(String replId, String gtidset, long maxGap) {
            SyncRequest request = new SyncRequest();
            request.proto = ReplStage.ReplProto.XSYNC;
            request.replId = replId;
            request.slaveGtidSet = new GtidSet(gtidset);
            request.maxGap = maxGap;
            return request;
        }

        @Override
        public String toString() {
            if (proto.equals(ReplStage.ReplProto.PSYNC)) {
                return String.format("%s %s %d", proto.name(), replId, offset);
            } else {
                return String.format("%s %s %s MAXGAP %d", proto.name(), replId, slaveGtidSet, maxGap);
            }
        }
    }

}
