package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.Xsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public class XsyncHandler extends AbstractSyncCommandHandler {

    // xsync <sidno interested> <gtid.set excluded> [vc excluded]
    protected void innerDoHandle(final String[] args, final RedisSlave redisSlave, RedisKeeperServer redisKeeperServer) throws IOException {
        KeeperConfig keeperConfig = redisKeeperServer.getKeeperConfig();
        KeeperRepl keeperRepl = redisKeeperServer.getKeeperRepl();

        Set<String> interestedSids = new HashSet<>(Arrays.asList(args[0].split(Xsync.SIDNO_SEPARATOR)));
        GtidSet reqExcludedGtidSet = new GtidSet(args[1]);

        GtidSet localBeginGtidSet = keeperRepl.getBeginGtidSet().filterGtid(interestedSids);
        GtidSet localEndGtidSet = keeperRepl.getEndGtidSet().filterGtid(interestedSids);

        GtidSet neededGtidSet = localEndGtidSet.subtract(reqExcludedGtidSet);
        GtidSet missingGtidSet = localBeginGtidSet.retainAll(neededGtidSet);

        if (!missingGtidSet.isEmpty()) {
            logger.info("[innerDoHandle][neededGtidSet is excluded][req-excluded loc-begin loc-end] {} {} {}",
                    reqExcludedGtidSet, localBeginGtidSet, localEndGtidSet);
            redisSlave.getRedisKeeperServer().getKeeperMonitor().getKeeperStats().increatePartialSyncError();
            doFullSync(redisSlave);
        } else if (localEndGtidSet.isContainedWithin(reqExcludedGtidSet)) {
            logger.info("[innerDoHandle][neededGtidSet not contain][req-excluded loc-excluded loc-end] {} {} {}",
                    reqExcludedGtidSet, localBeginGtidSet, localEndGtidSet);
            doPartialSync(redisSlave, interestedSids, reqExcludedGtidSet);
        } else {
            // TODO: use gtid missing count instead of data length?
            int dataNeededSyncApproximately = 1;
            if (dataNeededSyncApproximately > keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb()) {
                logger.info("[innerDoHandle][too much commands to transfer] {} > {}", dataNeededSyncApproximately,
                        keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb());
                doFullSync(redisSlave);
            } else {
                logger.info("[innerDoHandle][do partial sync] {} < {}", dataNeededSyncApproximately,
                        keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb());
                doPartialSync(redisSlave, interestedSids, reqExcludedGtidSet);
            }
        }

    }

    // +CONTINUE
    protected void doPartialSync(RedisSlave redisSlave, Set<String> interestedSid, GtidSet excludedGtidSet) {
        logger.info("[doPartialSync] {}", redisSlave);
        SimpleStringParser simpleStringParser = new SimpleStringParser(Xsync.PARTIAL_SYNC);

        redisSlave.sendMessage(simpleStringParser.format());
        redisSlave.markPsyncProcessed();

        redisSlave.beginWriteCommands(excludedGtidSet.filterGtid(interestedSid));
        redisSlave.partialSync();

        redisSlave.getRedisKeeperServer().getKeeperMonitor().getKeeperStats().increatePartialSync();
    }

    @Override
    public String[] getCommands() {
        return new String[]{"xsync"};
    }

}

