package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMergeEnd;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMergeStart;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParser;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultBroadcastCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultDataCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultExecCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultMultiCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 18:21
 */
public class DefaultCommandDispatcher extends AbstractInstanceComponent implements ApplierCommandDispatcher {

    @InstanceDependency
    public ApplierSequenceController sequenceController;

    @InstanceDependency
    public AsyncRedisClient client;

    @InstanceDependency
    public RedisOpParser parser;

    @InstanceDependency
    public ExecutorService stateThread;

    @InstanceDependency
    public AtomicReference<GtidSet> gtid_executed;

    /* why not a global resource */
    @VisibleForTesting
    RdbParser<?> rdbParser;

    @VisibleForTesting
    Set<String> receivedSids;

    @VisibleForTesting
    GtidSet gtid_received;

    public DefaultCommandDispatcher() {
        this.rdbParser = new DefaultRdbParser();
        this.rdbParser.registerListener(this);

        this.receivedSids = new HashSet<>();
    }

    @VisibleForTesting
    void resetState(GtidSet gtidSet) {
        this.gtid_received = gtidSet;
        this.receivedSids = new HashSet<>();
        this.gtid_executed.set(gtidSet);
    }

    @Override
    public void onFullSync(GtidSet rdbGtidSet) {

        logger.info("[onFullSync] rdbGtidSet={}", rdbGtidSet);

        this.resetState(rdbGtidSet);
    }

    @Override
    public void beginReadRdb(EofType eofType, GtidSet rdbGtidSet) {

        logger.info("[beginReadRdb] eofType={}, rdbGtidSet={}", eofType, rdbGtidSet);

        //ctrip.merge_start
        sequenceController.submit(new DefaultBroadcastCommand(client, new RedisOpMergeStart()));
    }

    @Override
    public void onRdbData(ByteBuf rdbData) {
        try {
            rdbParser.read(rdbData);
        } catch (Throwable t){
            logger.error("[onRdbData] unlikely - error", t);
        }
    }

    @Override
    public void endReadRdb(EofType eofType, GtidSet rdbGtidSet) {

        logger.info("[endReadRdb] eofType={}, rdbGtidSet={}", eofType, rdbGtidSet);

        //ctrip.merge_start [gtid_set]
        sequenceController.submit(new DefaultBroadcastCommand(client, new RedisOpMergeEnd(rdbGtidSet.toString())));
    }

    @Override
    public void onContinue(GtidSet gtidSetExcluded) {
        logger.info("[onContinue]");
        this.resetState(gtidSetExcluded);
    }

    @Override
    public void onCommand(Object[] rawCmdArgs) {
        try {
            onRedisOp(parser.parse(rawCmdArgs));
        } catch (Throwable unlikely) {
            logger.error("[onCommand] unlikely - when doing partial sync]", unlikely);
        }
    }

    public void updateGtidState(String gtid) {

        if (null == gtid) {
            return;
        }

        Pair<String, Long> parsed = Objects.requireNonNull(GtidSet.parseGtid(gtid));

        if (receivedSids.add(parsed.getKey())) {
            //sid first received
            gtid_received.rise(gtid);

            stateThread.execute(()->{
                logger.debug("[updateGtidState] rise gtid {} to gtid_executed {}", gtid, gtid_executed.get());
                gtid_executed.get().rise(gtid);
            });
        } else {
            //sid already received, transactionId may leap
            long last = gtid_received.rise(gtid);

            for (long i = last + 1; i < parsed.getValue(); i++) {
                long leaped = i;
                stateThread.execute(()->{
                    logger.debug("[updateGtidState] add leap gtid {}:{} to gtid_executed {}", parsed.getKey(), leaped, gtid_executed.get());
                    gtid_executed.get().add(GtidSet.composeGtid(parsed.getKey(), leaped));
                });
            }
        }
    }

    protected int toInt(byte[] value) {
        int rt = 0;
        for (byte b : value) {
            int add = b - '0';
            rt = rt * 10;
            rt += add;
        }
        return rt;
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {

        logger.debug("[onRedisOp] redisOpType={}, gtid={}", redisOp.getOpType(), redisOp.getOpGtid());

        if (RedisOpType.PING.equals(redisOp.getOpType())) {
            return;
        }
        if (RedisOpType.SELECT.equals(redisOp.getOpType())) {
            try {
                int db = toInt(redisOp.buildRawOpArgs()[1]);
                client.selectDB(db);
            } catch (Throwable unlikely) {
                logger.error("[onRedisOp] unlikely - fail to select db : {}", Arrays.toString(redisOp.buildRawOpArgs()[1]));
                logger.error("[onRedisOp] unlikely - fail to select db]", unlikely);
            }
            return;
        }

        updateGtidState(redisOp.getOpGtid());

        if (redisOp.getOpType().equals(RedisOpType.MULTI)) {
            sequenceController.submit(new DefaultMultiCommand(client, redisOp));
        } else if (redisOp.getOpType().equals(RedisOpType.EXEC)) {
            sequenceController.submit(new DefaultExecCommand(client, redisOp));
        } else {
            sequenceController.submit(new DefaultDataCommand(client, redisOp));
        }
    }

    @Override
    public void onAux(String key, String value) {

    }

    @Override
    public void onFinish(RdbParser<?> parser) {

    }
}
