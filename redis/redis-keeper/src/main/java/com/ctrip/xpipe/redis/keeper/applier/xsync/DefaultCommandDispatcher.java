package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParser;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultDataCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultExecCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultMultiCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
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
    public AtomicReference<GtidSet> gtid_received;

    @InstanceDependency
    public AtomicReference<GtidSet> gtid_executed;

    @InstanceDependency
    public ScheduledExecutorService stateThread;

    private RdbParser<?> rdbParser;

    private boolean firstReceived = true;

    public DefaultCommandDispatcher() {
        this.rdbParser = new DefaultRdbParser();
        this.rdbParser.registerListener(this);
    }

    @Override
    public void onFullSync(GtidSet rdbGtidSet) {

    }

    @Override
    public void beginReadRdb(EofType eofType, GtidSet rdbGtidSet) {
        //merge.start
    }

    @Override
    public void onRdbData(ByteBuf rdbData) {
        rdbParser.read(rdbData);
    }

    @Override
    public void endReadRdb(EofType eofType, GtidSet rdbGtidSet) {
        //merge.end [gtid]
        this.gtid_received.set(rdbGtidSet);
    }

    @Override
    public void onContinue() {

    }

    @Override
    public void onCommand(Object[] rawCmdArgs) {
        RedisOp redisOp = parser.parse(rawCmdArgs);
        logger.debug("[onCommand] redisOpType={}, gtid={}", redisOp.getOpType(), redisOp.getOpGtid());
        if (RedisOpType.PING.equals(redisOp.getOpType()) || RedisOpType.SELECT.equals(redisOp.getOpType())) {
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

    public void updateGtidState(String gtid) {

        if (firstReceived) {
            firstReceived = false;
            gtid_received.get().rise(gtid);

            stateThread.execute(()->{
                gtid_executed.get().rise(gtid);
            });
        } else {
            Pair<String, Long> parsed = Objects.requireNonNull(GtidSet.parseGtid(gtid));
            long last = gtid_received.get().rise(gtid);

            for (long i = last + 1; i < parsed.getValue(); i++) {
                long leaped = i;
                stateThread.execute(()->{
                    gtid_executed.get().add(GtidSet.composeGtid(parsed.getKey(), leaped));
                });
            }
        }
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {
        if (RedisOpType.PING.equals(redisOp.getOpType()) || RedisOpType.SELECT.equals(redisOp.getOpType())) {
            return;
        }
        sequenceController.submit(new DefaultDataCommand(client, redisOp));
    }

    @Override
    public void onAux(String key, String value) {

    }

    @Override
    public void onFinish(RdbParser<?> parser) {

    }
}
