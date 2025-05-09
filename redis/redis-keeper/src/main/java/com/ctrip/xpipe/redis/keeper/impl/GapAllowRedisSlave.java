package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.BacklogOffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.utils.StringUtil;

import static com.ctrip.xpipe.redis.core.protocal.GapAllowedSync.XFULL_SYNC;
import static com.ctrip.xpipe.redis.core.protocal.Psync.FULL_SYNC;

public class GapAllowRedisSlave extends DefaultRedisSlave {

    public GapAllowRedisSlave(RedisClient<RedisKeeperServer> redisClient) {
        super(redisClient);
    }

    @Override
    public void beginWriteRdb(EofType eofType, ReplicationProgress<?> rdbProgress) {
        // TODO: init end backlog offset in other proper place
        if (rdbProgress instanceof BacklogOffsetReplicationProgress) {
            long backlogOffset = ((BacklogOffsetReplicationProgress) rdbProgress).getProgress();
            KeeperRepl keeperRepl = getRedisServer().getKeeperRepl();
            ReplStage curStage = keeperRepl.currentStage();
            ReplStage preStage = keeperRepl.preStage();

            if (backlogOffset + 1 >= curStage.getBegOffsetBacklog()) {
                // do nothing
            } else if (null != preStage && backlogOffset + 1 >= preStage.getBegOffsetBacklog()) {
                ((BacklogOffsetReplicationProgress) rdbProgress).setEndBacklogOffset(curStage.getBegOffsetBacklog() - 1);
            }
        }

        super.beginWriteRdb(eofType, rdbProgress);
    }

    @Override
    protected String buildMarkBeforeFsync(ReplicationProgress<?> rdbProgress) {
        if (rdbProgress instanceof BacklogOffsetReplicationProgress) {
            long backlogOffset = ((BacklogOffsetReplicationProgress) rdbProgress).getProgress();
            KeeperRepl keeperRepl = getRedisServer().getKeeperRepl();
            ReplStage curStage = keeperRepl.currentStage();
            ReplStage preStage = keeperRepl.preStage();

            if (backlogOffset + 1 >= curStage.getBegOffsetBacklog()) {
                return buildRespWithReplStage(backlogOffset, curStage);
            } else if (null != preStage && backlogOffset + 1 >= preStage.getBegOffsetBacklog()) {
                return buildRespWithReplStage(backlogOffset, preStage);
            } else {
                throw new UnsupportedOperationException("Rdb start from replStage before last is not allowed!");
            }
        } else {
            throw new UnsupportedOperationException("Progress not supported: " + rdbProgress);
        }
    }

    private String buildRespWithReplStage(long backlogOffset, ReplStage replStage) {
        if (replStage.getProto() == ReplStage.ReplProto.PSYNC) {
            return StringUtil.join(" ", FULL_SYNC, replStage.getReplId(),
                    replStage.backlogOffset2ReplOffset(backlogOffset));
        } else {
            GtidSet lost = replStage.getGtidLost();
            return StringUtil.join(" ", XFULL_SYNC, "GTID.LOST", lost.isEmpty() ? "\"\"" : lost,
                    "MASTER.UUID", replStage.getMasterUuid(), "REPLID", replStage.getReplId(),
                    "REPLOFF", replStage.backlogOffset2ReplOffset(backlogOffset));
        }
    }

    @Override
    public boolean supportProgress(Class<? extends ReplicationProgress<?>> clazz) {
        return clazz.equals(BacklogOffsetReplicationProgress.class) || super.supportProgress(clazz);
    }

}
