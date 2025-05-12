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
                SyncRequest request = parseRequest(args, redisSlave);
                if (null == request) {
                    logger.warn("[doHandle] request parse fail");
                    return;
                }

                SyncAction action = null;
                try {
                    action = anaRequest(request, redisKeeperServer, redisSlave);
                } catch (Exception e) {
                    action = SyncAction.full("anaRequest fail");
                }
                if (null == action) {
                    logger.warn("[doHandle] request analyze fail");
                    return;
                }

                runAction(action, redisKeeperServer, redisSlave);
            }
        });
    }

    protected abstract SyncRequest parseRequest(final String[] args, RedisSlave redisSlave);

    protected SyncAction anaRequest(SyncRequest request, RedisKeeperServer redisKeeperServer, RedisSlave slave) throws Exception {
        KeeperRepl keeperRepl = redisKeeperServer.getKeeperRepl();
        ReplStage preStage = keeperRepl.preStage();
        ReplStage curStage = keeperRepl.currentStage();

        if (null == preStage && null == curStage) {
            slave.sendMessage(new RedisErrorParser(new NoMasterlinkRedisError("Can't SYNC while replicationstore fresh")).format());
            return null;
        }

        if (request.requestFull()) return SyncAction.full("req fsync");

        if (null != curStage && curStage.getProto() == request.proto) {
            if (request.proto == ReplStage.ReplProto.PSYNC) {
                return anaPSync(request, curStage, keeperRepl, -1);
            } else {
                XSyncContinue xsyncCont = null;
                xsyncCont = redisKeeperServer.locateContinueGtidSet(request.slaveGtidSet);
                return anaXSync(request, curStage, xsyncCont, -1);
            }
        } else if (null != curStage && null != preStage && preStage.getProto() == request.proto) {
            // 先判断临界点
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
                return anaPSync(request, preStage, keeperRepl, curStage.getBegOffsetBacklog() - 1);
            } else {
                return anaXSync(request, preStage, xsyncCont, curStage.getBegOffsetBacklog() - 1);
            }
        }

        return SyncAction.full("no repl stage match");
    }

    private SyncAction switchProto(ReplStage replStage) {
        if (replStage.getProto() == ReplStage.ReplProto.PSYNC) {
            return SyncAction.Continue(replStage, -1, true, replStage.getReplId(), replStage.getBegOffsetRepl());
        } else {
            return SyncAction.XContinue(replStage, -1, true, replStage.getBeginGtidset(), replStage.getBegOffsetBacklog(), null);
        }
    }

    protected SyncAction anaPSync(SyncRequest request, ReplStage psyncStage, KeeperRepl keeperRepl, long stageEndBacklogOffset) {
        // 使用replId replOffset, replId2 replOffset2
        long reqBacklogOffset = psyncStage.replOffset2BacklogOffset(request.offset);
        String keeperReplId = psyncStage.getReplId();
        String keeperReplId2 = psyncStage.getReplId2();
        long keeperOffset2 = psyncStage.getSecondReplIdOffset();
        long backlogBeginOffset = Math.max(psyncStage.getBegOffsetBacklog(),keeperRepl.backlogBeginOffset());

        if (request.replId.equalsIgnoreCase(keeperReplId) || (request.replId.equalsIgnoreCase(keeperReplId2) && request.offset <= keeperOffset2)) {
            if (reqBacklogOffset < backlogBeginOffset) {
                return SyncAction.full(String.format("[request offset miss][req: %d, sup:%d]", reqBacklogOffset, backlogBeginOffset));
            } else {
                // TODO: keeper wait, limit transform data
                return SyncAction.Continue(psyncStage, stageEndBacklogOffset, false, keeperReplId, request.offset);
            }
        } else {
            return SyncAction.full("replId mismatch");
        }
    }

    protected SyncAction anaXSync(SyncRequest request, ReplStage xsyncStage, XSyncContinue xsyncCont, long stageEndBacklogOffset) {
        GtidSet lost = xsyncStage.getGtidLost();
        GtidSet cont = xsyncCont.getContinueGtidSet();
        GtidSet req = request.slaveGtidSet;

        if ("*".equals(request.replId) || xsyncStage.getReplId().equalsIgnoreCase(request.replId)) {
            GtidSet masterGtidSet = cont.union(lost);
            GtidSet gap = masterGtidSet.symmetricDiff(req);
            GtidSet masterLost = req.subtract(masterGtidSet);
            int gapCnt = gap.itemCnt();
            // TODO: keeper wait transform limit
            if (request.maxGap >= 0 && request.maxGap < gapCnt) {
                return SyncAction.full(String.format("[gap][%d > %d]", gapCnt, request.maxGap));
            } else {
                return SyncAction.XContinue(xsyncStage, stageEndBacklogOffset, false, masterGtidSet, xsyncCont.getBacklogOffset(), masterLost);
            }
        } else {
            return SyncAction.full("replId mismatch");
        }
    }

    protected void runAction(SyncAction action, RedisKeeperServer keeperServer, RedisSlave slave) {
        if (action.isFull()) {
            try {
                logger.info("[runAction] {}", action.fullCause);
                keeperServer.fullSyncToSlave(slave);
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

            SimpleStringParser resp = null;
            if (action.replStage.getProto() == ReplStage.ReplProto.PSYNC) {
                resp = new SimpleStringParser(String.format("%s %s", PARTIAL_SYNC, action.replId)
                        + (action.protoSwitch ? " "+(action.replOffset - 1):""));
            } else {
                resp = new SimpleStringParser(String.format("XCONTINUE GTID.SET %s MASTER.UUID %s REPLID %s REPLOFF %d",
                        action.gtidSet.toString(), action.replStage.getMasterUuid(), action.replId, action.replOffset - 1));
            }

            slave.sendMessage(resp.format());
            slave.beginWriteCommands(new BacklogOffsetReplicationProgress(action.backlogOffset, action.backlogEndOffset));
        }
    }

    protected static class SyncAction {

        boolean full = false;

        String fullCause;

        boolean protoSwitch = false;

        String replId;

        long replOffset = -1;

        long backlogOffset = -1;

        long backlogEndOffset = -1;

        ReplStage replStage;

        GtidSet gtidSet;

        GtidSet masterLost;

        public static SyncAction full(String fullCause) {
            SyncAction action = new SyncAction();
            action.full = true;
            action.fullCause = fullCause;
            return action;
        }

        public static SyncAction XContinue(ReplStage replStage, long stageEndBacklogOffset, boolean protoSwitch, GtidSet contGtidSet, long backlogOffset, GtidSet masterLost) {
            SyncAction action = new SyncAction();
            action.replStage = replStage;
            action.protoSwitch = protoSwitch;
            action.gtidSet = contGtidSet;
            action.replId = replStage.getReplId();
            action.replOffset = replStage.backlogOffset2ReplOffset(backlogOffset);
            action.backlogOffset = backlogOffset;
            action.backlogEndOffset = stageEndBacklogOffset;
            action.masterLost = masterLost;
            return action;
        }

        public static SyncAction Continue(ReplStage replStage, long stageEndBacklogOffset, boolean protoSwitch, String replId, long offset) {
            SyncAction action = new SyncAction();
            action.replStage = replStage;
            action.backlogEndOffset = stageEndBacklogOffset;
            action.protoSwitch = protoSwitch;
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

        public boolean requestFull() {
            return this.replId.equals("?");
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
