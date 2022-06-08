package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.Xsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import io.netty.buffer.ByteBuf;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 17:20
 */
public class DefaultXsyncReplication extends AbstractInstanceComponent implements ApplierXsyncReplication {

    @InstanceDependency
    public RedisOpParser parser;

//    protected Xsync xsync;

    @Override
    protected void doStart() throws Exception {
//        xsync = new DefaultXsync(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", server.getPort())),
//                gtidSet, null, scheduled);
//        redisOps = new ArrayList<>();
//        xsync.addXsyncObserver(this);
    }

    @Override
    public void onFullSync(GtidSet rdbGtidSet) {

    }

    @Override
    public void beginReadRdb(EofType eofType, GtidSet rdbGtidSet) {

    }

    @Override
    public void onRdbData(ByteBuf rdbData) {

    }

    @Override
    public void endReadRdb(EofType eofType, GtidSet rdbGtidSet) {

    }

    @Override
    public void onContinue() {

    }

    @Override
    public void onCommand(Object[] rawCmdArgs) {

    }
}
