package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.Xsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.core.store.GtidSetReplicationProgress;
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

    @Override
    protected RedisSlave becomeSlave(RedisClient<?> redisClient) {
        return redisClient.becomeXSlave();
    }

    // xsync <sidno interested> <gtid.set excluded> [vc excluded]
    protected void innerDoHandle(final String[] args, final RedisSlave redisSlave, RedisKeeperServer redisKeeperServer) throws IOException {

        redisKeeperServer.startIndexing();

        KeeperConfig keeperConfig = redisKeeperServer.getKeeperConfig();

        KeeperRepl keeperRepl = redisKeeperServer.getKeeperRepl();

        Set<String> interestedSids = new HashSet<>(Arrays.asList(args[0].split(Xsync.SIDNO_SEPARATOR)));
        GtidSet reqExcludedGtidSet = new GtidSet(args[1]);

        if (reqExcludedGtidSet.isZero()) {
            logger.info("[innerDoHandle][reqExcludedGtidSet is zero]");
            doFullSync(redisSlave);
            return;
        }

        GtidSet localBegin = keeperRepl.getBeginGtidSet();
        GtidSet localEnd = keeperRepl.getEndGtidSet();

        if (null == localBegin || localBegin.isEmpty()) {
            logger.info("[innerDoHandle][localBegin is null]");
            doFullSync(redisSlave);
            return;
        }

        GtidSet filteredLocalBegin = localBegin.filterGtid(interestedSids);
        GtidSet filteredLocalEnd = localEnd.filterGtid(interestedSids);

        GtidSet neededGtidSet = filteredLocalEnd.subtract(reqExcludedGtidSet);
        GtidSet missingGtidSet = filteredLocalBegin.retainAll(neededGtidSet);

        if (!missingGtidSet.isEmpty() && !missingGtidSet.isZero()) {
            logger.info("[innerDoHandle][neededGtidSet is excluded][req-excluded loc-begin loc-end] {} {} {}",
                    reqExcludedGtidSet, filteredLocalBegin, filteredLocalEnd);
            redisSlave.getRedisServer().getKeeperMonitor().getKeeperStats().increatePartialSyncError();
            doFullSync(redisSlave);
        } else if (filteredLocalEnd.isContainedWithin(reqExcludedGtidSet)) {
            logger.info("[innerDoHandle][neededGtidSet not contain][do partial sync][req-excluded loc-excluded loc-end] {} {} {}",
                    reqExcludedGtidSet, filteredLocalBegin, filteredLocalEnd);
            doPartialSync(redisSlave, interestedSids, reqExcludedGtidSet);
        } else {
            if (filteredLocalEnd.lwmDistance(reqExcludedGtidSet) < keeperConfig.getReplicationStoreMaxLWMDistanceToTransferBeforeCreateRdb()) {
                logger.info("[innerDoHandle][neededGtidSet contain][do partial sync][req-excluded loc-excluded loc-end] {} {} {} {}",
                        reqExcludedGtidSet, filteredLocalBegin, filteredLocalEnd, keeperConfig.getReplicationStoreMaxLWMDistanceToTransferBeforeCreateRdb());
                doPartialSync(redisSlave, interestedSids, reqExcludedGtidSet);
            } else {
                logger.info("[innerDoHandle][too much commands to transfer] {} {} {} {}",
                        reqExcludedGtidSet, filteredLocalBegin, filteredLocalEnd, keeperConfig.getReplicationStoreMaxLWMDistanceToTransferBeforeCreateRdb());
                redisSlave.getRedisServer().getKeeperMonitor().getKeeperStats().increatePartialSyncError();
                doFullSync(redisSlave);
            }
        }
    }

    // +CONTINUE
    protected void doPartialSync(RedisSlave redisSlave, Set<String> interestedSid, GtidSet excludedGtidSet) {
        logger.info("[doPartialSync] {}", redisSlave);
        SimpleStringParser simpleStringParser = new SimpleStringParser(Xsync.PARTIAL_SYNC);

        redisSlave.sendMessage(simpleStringParser.format());
        redisSlave.markPsyncProcessed();

        redisSlave.beginWriteCommands(new GtidSetReplicationProgress(excludedGtidSet.filterGtid(interestedSid)));
        redisSlave.partialSync();

        ((RedisKeeperServer)redisSlave.getRedisServer()).getKeeperMonitor().getKeeperStats().increatePartialSync();
    }

    @Override
    public String[] getCommands() {
        return new String[]{"xsync"};
    }

}

