package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultPsync;
import com.ctrip.xpipe.redis.core.store.BacklogOffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.utils.StringUtil;

public class GapAllowRedisSlave extends DefaultRedisSlave {

    public GapAllowRedisSlave(RedisClient<RedisKeeperServer> redisClient) {
        super(redisClient);
    }

    @Override
    protected String buildMarkBeforeFsync(ReplicationProgress<?> rdbProgress) {
        if (rdbProgress instanceof BacklogOffsetReplicationProgress) {
            long backlogOffset = ((BacklogOffsetReplicationProgress) rdbProgress).getProgress();
            KeeperRepl keeperRepl = getRedisServer().getKeeperRepl();
            ReplStage curStage = keeperRepl.currentStage();
            ReplStage preStage = keeperRepl.preStage();

            if (backlogOffset > curStage.getBegOffsetBacklog()) {
                return buildRespWithReplStage(backlogOffset, curStage);
            } else if (null != preStage && backlogOffset > preStage.getBegOffsetBacklog()) {
                return buildRespWithReplStage(backlogOffset, preStage);
            } else {
                // TODO: specified exception, handle ancient rdb
                throw new XpipeRuntimeException("");
            }

        } else {
            return super.buildMarkBeforeFsync(rdbProgress);
        }
    }

    private String buildRespWithReplStage(long backlogOffset, ReplStage replStage) {
        if (replStage.getProto() == ReplStage.ReplProto.PSYNC) {
            return StringUtil.join(" ", DefaultPsync.FULL_SYNC, replStage.getReplId(),
                    replStage.backlogOffset2ReplOffset(backlogOffset));
        } else {
            RedisKeeperServer keeperServer = getRedisServer();
            KeeperRepl keeperRepl = keeperServer.getKeeperRepl();
            return StringUtil.join(" ", "XFULLRESYNC", keeperRepl.getGtidSetLost(),
                    "MASTER.UUID", replStage.getMasterUuid(), "REPLID", replStage.getReplId(),
                    "REPLOFF", replStage.backlogOffset2ReplOffset(backlogOffset));
        }
    }

}
