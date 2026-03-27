package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.exception.RdbParseEmptyKeyException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.Listpack;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author TB
 * @date 2026/3/18 16:33
 */
public class RdbHashListpackParser extends AbstractRdbParser<Map<byte[], byte[]>> implements RdbParser<Map<byte[],byte[]>> {

    private RdbParseContext context;

    private RdbParser<byte[]> rdbStringParser;

    private byte[] temp;

    private Listpack listpack;

    private STATE state = STATE.READ_INIT;

    private static final Logger logger = LoggerFactory.getLogger(RdbHashZipListParser.class);

    enum STATE {
        READ_INIT,
        READ_AS_STRING,
        DECODE_LISTPACK,
        READ_END
    }

    public RdbHashListpackParser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public Map<byte[], byte[]> read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    listpack = null;
                    state = STATE.READ_AS_STRING;
                    break;

                case READ_AS_STRING:
                    temp = rdbStringParser.read(byteBuf);
                    if (null == temp) {
                        break;
                    }
                    rdbStringParser.reset();
                    state = STATE.DECODE_LISTPACK;

                case DECODE_LISTPACK:
                    if (listpack == null) {
                        listpack = new Listpack(temp);
                        if (listpack.size() == 0 || listpack.size() % 2 != 0) {
                            throw new RdbParseEmptyKeyException(context.getKey() + " hash listpack invalid size");
                        }
                    }
                    state = STATE.READ_END;

                case READ_END:
                default:
                    temp = null;

            }

            if (isFinish()) {
                propagateCmdIfNeed(listpack);
                propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
            }
        }

        if (isFinish()) return listpack.convertToMap();
        else return null;
    }

    private void propagateCmdIfNeed(Listpack listpack) {
        if (null == context.getKey() || null == listpack) return;

        Map<byte[], byte[]> map = listpack.convertToMap();
        RedisKey key = context.getKey();
        int i = 0;
        for (Map.Entry<byte[], byte[]> entry: map.entrySet()) {
            if(i == map.size()-1){
                notifyRedisOp(new RedisOpSingleKey(
                        RedisOpType.HSET,
                        new byte[][] {RedisOpType.HSET.name().getBytes(), key.get(), entry.getKey(), entry.getValue()},
                        key, entry.getKey(),true));
            }else {
                notifyRedisOp(new RedisOpSingleKey(
                        RedisOpType.HSET,
                        new byte[][] {RedisOpType.HSET.name().getBytes(), key.get(), entry.getKey(), entry.getValue()},
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
        this.listpack = null;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}

