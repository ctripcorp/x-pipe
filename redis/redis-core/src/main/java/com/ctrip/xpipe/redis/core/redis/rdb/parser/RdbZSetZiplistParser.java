package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.Ziplist;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author lishanglin
 * date 2022/6/18
 */
public class RdbZSetZiplistParser extends AbstractRdbParser<Map<byte[], byte[]>> implements RdbParser<Map<byte[], byte[]>> {

    private RdbParseContext context;

    private RdbParser<byte[]> rdbStringParser;

    private byte[] temp;

    private Ziplist ziplist;

    private STATE state = STATE.READ_INIT;

    private static final Logger logger = LoggerFactory.getLogger(RdbZSetZiplistParser.class);

    enum STATE {
        READ_INIT,
        READ_AS_STRING,
        DECODE_ZIPLIST,
        READ_END
    }

    public RdbZSetZiplistParser(RdbParseContext parseContext) {
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
                    state = STATE.DECODE_ZIPLIST;

                case DECODE_ZIPLIST:
                    ziplist = new Ziplist(temp);
                    state = STATE.READ_END;

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
        int i = 0;
        for (Map.Entry<byte[], byte[]> entry: map.entrySet()) {
            if(i == map.size() - 1){
                notifyRedisOp(new RedisOpSingleKey(
                        RedisOpType.ZADD,
                        new byte[][] {RedisOpType.ZADD.name().getBytes(), key.get(), entry.getValue(), entry.getKey()},
                        key, entry.getKey(),true));
            }else {
                notifyRedisOp(new RedisOpSingleKey(
                        RedisOpType.ZADD,
                        new byte[][] {RedisOpType.ZADD.name().getBytes(), key.get(), entry.getValue(), entry.getKey()},
                        key, entry.getKey()));
            }
            i++;
        }
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        if (rdbStringParser != null) {
            rdbStringParser.reset();
        }
        this.state = STATE.READ_INIT;
        this.temp = null;
        this.ziplist = null;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
