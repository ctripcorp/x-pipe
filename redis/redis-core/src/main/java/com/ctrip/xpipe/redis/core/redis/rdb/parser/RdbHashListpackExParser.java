package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.exception.RdbParseEmptyKeyException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.Listpack;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Triple;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @author TB
 * @date 2026/3/16 15:00
 */
public class RdbHashListpackExParser extends AbstractRdbParser<List<Triple<byte[],byte[],Long>>> implements RdbParser<List<Triple<byte[],byte[],Long>>> {

    private RdbParseContext context;

    private RdbParser<byte[]> rdbStringParser;

    private enum State {
        READ_INIT,
        READ_MIN_EXPIRE,
        READ_LISTPACK,
        DECODE_LISTPACK,
        READ_END
    }

    private State state = State.READ_INIT;
    private long minExpire;
    private byte[] listpackBytes;
    private Listpack listpack;

    private static final Logger logger = LoggerFactory.getLogger(RdbHashListpackExParser.class);

    public RdbHashListpackExParser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public List<Triple<byte[],byte[],Long>> read(ByteBuf byteBuf) {
        while (!isFinish() && byteBuf.readableBytes() > 0) {
            switch (state) {
                case READ_INIT:
                    minExpire = 0;
                    listpackBytes = null;
                    listpack = null;
                    state = State.READ_MIN_EXPIRE;
                    break;

                case READ_MIN_EXPIRE:
                    if (byteBuf.readableBytes() < 8) {
                        return null;
                    }
                    minExpire = byteBuf.readLongLE(); // 小端序
                    state = State.READ_LISTPACK;
                    break;

                case READ_LISTPACK:
                    listpackBytes = rdbStringParser.read(byteBuf);
                    if (listpackBytes == null) {
                        break;
                    }
                    rdbStringParser.reset();
                    state = State.DECODE_LISTPACK;

                case DECODE_LISTPACK:
                    if (listpack == null) {
                        listpack = new Listpack(listpackBytes);
                        if (listpack.size() == 0 || listpack.size() % 3 != 0) {
                            throw new RdbParseEmptyKeyException(context.getKey() + " hash listpack invalid size");
                        }
                    }
                    state = State.READ_END;

                case READ_END:
                    // 已完成，退出循环
                    break;

                default:
                    break;
            }
        }

        if (isFinish()) {
            propagateCmdIfNeed(listpack);
            propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
        }

        return isFinish() ? listpack.convertToTriples() : null;
    }

    private void propagateCmdIfNeed(Listpack listPack) {
        if (null == context.getKey() || null == listPack) return;

        RedisKey key = context.getKey();
        List<Triple<byte[],byte[],Long>> tripleList = listPack.convertToTriples();
        boolean isLast = false;
        for(int i = 0;i<tripleList.size();i++) {
            RedisOpType redisOp = RedisOpType.HSETEX;
            long ttl = tripleList.get(i).getLast();
            if(i == listPack.size() - 1 ) {
                isLast = true;
            }
            if(ttl > 0){
                notifyRedisOp(new RedisOpSingleKey(
                        redisOp,
                        new byte[][]{
                                redisOp.name().getBytes(),
                                key.get(),
                                HASH_PXAT,
                                (ttl+"").getBytes(),
                                HASH_FIELDS,
                                HASH_1,
                                tripleList.get(i).getFirst(),
                                tripleList.get(i).getMiddle()
                        },
                        key, tripleList.get(i).getFirst(), isLast
                ));
            }
        }
    }

    private byte[] shift(long v) {
        byte[] b = new byte[8];
        for (int i = 0; i < 8; i++) {
            b[i] = (byte) (v >>> (i * 8));
        }
        return b;
    }

    @Override
    public boolean isFinish() {
        return state == State.READ_END;
    }

    @Override
    public void reset() {
        super.reset();
        if (rdbStringParser != null) {
            rdbStringParser.reset();
        }
        state = State.READ_INIT;
        listpackBytes = null;
        listpack = null;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
