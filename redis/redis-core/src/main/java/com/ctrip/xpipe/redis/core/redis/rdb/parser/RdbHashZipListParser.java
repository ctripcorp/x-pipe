package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.ziplist.Ziplist;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author lishanglin
 * date 2022/6/16
 */
public class RdbHashZipListParser extends AbstractRdbParser<Map<byte[], byte[]>> implements RdbParser<Map<byte[],byte[]>> {

    private RdbParseContext context;

    private RdbParser<byte[]> rdbStringParser;

    private byte[] temp;

    private Ziplist ziplist;

    private STATE state = STATE.READ_INIT;

    private static final Logger logger = LoggerFactory.getLogger(RdbHashZipListParser.class);

    enum STATE {
        READ_INIT,
        READ_AS_STRING,
        ZIPLIST_DECODE,
        READ_END
    }

    public RdbHashZipListParser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public Map<byte[], byte[]> read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    ziplist = null;
                    state = STATE.READ_AS_STRING;
                    break;

                case READ_AS_STRING:
                    temp = rdbStringParser.read(byteBuf);
                    if (null == temp) {
                        break;
                    }
                    rdbStringParser.reset();
                    state = STATE.ZIPLIST_DECODE;

                case ZIPLIST_DECODE:
                    ziplist = new Ziplist(temp);
                    state = STATE.READ_END;
                    break;

                case READ_END:
                default:
                    temp = null;

            }

            if (isFinish()) {
                propagateCmdIfNeed(ziplist);
                propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
            }
        }

        if (isFinish()) return ziplist.convertToMap();
        else return null;
    }

    private void propagateCmdIfNeed(Ziplist ziplist) {
        if (null == context.getKey() || null == ziplist) return;

        Map<byte[], byte[]> map = ziplist.convertToMap();
        RedisKey key = context.getKey();
        for (Map.Entry<byte[], byte[]> entry: map.entrySet()) {
            notifyRedisOp(new RedisOpSingleKey(
                    RedisOpType.HSET,
                    new byte[][] {RedisOpType.HSET.name().getBytes(), key.get(), entry.getKey(), entry.getValue()},
                    key, entry.getKey()));
        }
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        this.state = STATE.READ_INIT;
        this.temp = null;
        this.ziplist = null;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
