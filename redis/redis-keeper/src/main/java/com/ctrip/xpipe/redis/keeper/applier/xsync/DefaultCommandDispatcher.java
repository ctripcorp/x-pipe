package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultDataCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpDataCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;

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

    @Override
    public void onFullSync(GtidSet rdbGtidSet) {

    }

    @Override
    public void beginReadRdb(EofType eofType, GtidSet rdbGtidSet) {

    }

    @Override
    public void onRdbData(Object rdbData) {

    }

    @Override
    public void endReadRdb(EofType eofType, GtidSet rdbGtidSet) {

    }

    @Override
    public void onContinue() {

    }

    @Override
    public void onCommand(Object[] rawCmdArgs) {
        RedisOp redisOp = parser.parse(rawCmdArgs);
        if (RedisOpType.PING.equals(redisOp.getOpType()) || RedisOpType.SELECT.equals(redisOp.getOpType())) {
            return;
        }

        /* TODO: deal with leaping gtid when keeper filter data */

        RedisOpDataCommand<Boolean> command = new DefaultDataCommand(client, redisOp);
        switch (command.type()) {
            case MULTI:
//                inTransaction = true;
            case EXEC:
//                inTransaction = false;
            default:
                sequenceController.submit(command);
        }
    }
}
