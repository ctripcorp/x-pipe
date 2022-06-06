package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.AuxOnlyRdbParser;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant.REDIS_RDB_AUX_KEY_GTID;

/**
 * @author lishanglin
 * date 2022/6/6
 */
public class GtidRdbStore extends DefaultRdbStore implements RdbStore, RdbParseListener {

    private RdbParser<?> rdbParser;

    private static final Logger logger = LoggerFactory.getLogger(GtidRdbStore.class);

    public GtidRdbStore(File file, long rdbOffset, EofType eofType) throws IOException {
        super(file, rdbOffset, eofType);
        this.rdbParser = new AuxOnlyRdbParser();
        this.rdbParser.registerListener(this);
    }

    @Override
    public int writeRdb(ByteBuf byteBuf) throws IOException {
        makeSureOpen();

        if (!rdbParser.isFinish()) rdbParser.read(byteBuf.slice());
        int wrote = ByteBufUtils.writeByteBufToFileChannel(byteBuf, channel);
        return wrote;
    }

    @Override
    public void endRdb() {
        super.endRdb();
        this.rdbParser.reset();
    }

    @Override
    public void failRdb(Throwable throwable) {
        super.failRdb(throwable);
        this.rdbParser.reset();
    }

    @Override
    public void onAux(String key, String value) {
        if (REDIS_RDB_AUX_KEY_GTID.equalsIgnoreCase(key)) {
            notifyListenersRdbGtidSet(value);
        }
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {

    }

    @Override
    public void onFinish(RdbParser<?> parser) {

    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
