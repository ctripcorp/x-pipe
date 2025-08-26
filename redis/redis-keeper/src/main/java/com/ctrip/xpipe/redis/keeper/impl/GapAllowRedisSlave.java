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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ctrip.xpipe.redis.core.protocal.GapAllowedSync.XFULL_SYNC;
import static com.ctrip.xpipe.redis.core.protocal.Psync.FULL_SYNC;

public class GapAllowRedisSlave extends DefaultRedisSlave {

    private final static Logger logger = LoggerFactory.getLogger(GapAllowRedisSlave.class);

    public GapAllowRedisSlave(RedisClient<RedisKeeperServer> redisClient) {
        super(redisClient);
    }

    @Override
    public void beginWriteRdb(EofType eofType, ReplicationProgress<?> rdbProgress) {
        // TODO: init end backlog offset in RdbStore on proto switch
        if (rdbProgress instanceof BacklogOffsetReplicationProgress) {
            long rdbContBacklogOffset = ((BacklogOffsetReplicationProgress) rdbProgress).getProgress();
            KeeperRepl keeperRepl = getRedisServer().getKeeperRepl();
            ReplStage curStage = keeperRepl.currentStage();
            ReplStage preStage = keeperRepl.preStage();

            if (rdbContBacklogOffset >= curStage.getBegOffsetBacklog()) {
                // do nothing
            } else if (null != preStage && rdbContBacklogOffset >= preStage.getBegOffsetBacklog()) {
                ((BacklogOffsetReplicationProgress) rdbProgress).setEndBacklogOffsetExcluded(curStage.getBegOffsetBacklog());
            }
        }

        super.beginWriteRdb(eofType, rdbProgress);
    }

    @Override
    protected String buildMarkBeforeFsync(ReplicationProgress<?> rdbProgress) {
        if (rdbProgress instanceof BacklogOffsetReplicationProgress) {
            long rdbContBacklogOffset = ((BacklogOffsetReplicationProgress) rdbProgress).getProgress();
            KeeperRepl keeperRepl = getRedisServer().getKeeperRepl();
            ReplStage curStage = keeperRepl.currentStage();
            ReplStage preStage = keeperRepl.preStage();

            if (rdbContBacklogOffset >= curStage.getBegOffsetBacklog()) {
                return buildRespWithReplStage(rdbContBacklogOffset, curStage);
            } else if (null != preStage && rdbContBacklogOffset >= preStage.getBegOffsetBacklog()) {
                return buildRespWithReplStage(rdbContBacklogOffset, preStage);
            } else {
                getLogger().info("[buildMarkBeforeFsync][cur:{}][pre:{}]", curStage, preStage);
                getLogger().warn("[buildMarkBeforeFsync][Rdb start from replStage before last] rdb:{}", rdbContBacklogOffset);
                throw new UnsupportedOperationException("Rdb start from replStage before last is not allowed!");
            }
        } else {
            throw new UnsupportedOperationException("Progress not supported: " + rdbProgress);
        }
    }

    private String buildRespWithReplStage(long rdbContBacklogOffset, ReplStage replStage) {
        if (replStage.getProto() == ReplStage.ReplProto.PSYNC) {
            return StringUtil.join(" ", FULL_SYNC, replStage.getReplId(),
                    replStage.backlogOffset2ReplOffset(rdbContBacklogOffset) - 1);
        } else {
            GtidSet lost = replStage.getGtidLost();
            return StringUtil.join(" ", XFULL_SYNC, "GTID.LOST", lost.isEmpty() ? "\"\"" : lost,
                    "MASTER.UUID", replStage.getMasterUuid(), "REPLID", replStage.getReplId(),
                    "REPLOFF", replStage.backlogOffset2ReplOffset(rdbContBacklogOffset) - 1);
        }
    }

    @Override
    public boolean supportProgress(Class<? extends ReplicationProgress<?>> clazz) {
        return clazz.equals(BacklogOffsetReplicationProgress.class) || super.supportProgress(clazz);
    }

    protected Logger getLogger() {
        return logger;
    }

}
