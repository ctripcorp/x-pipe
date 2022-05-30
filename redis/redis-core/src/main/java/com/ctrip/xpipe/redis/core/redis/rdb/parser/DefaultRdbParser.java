package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/5/28
 */
public class DefaultRdbParser extends AbstractRdbParser implements RdbParser {

    private RdbParseContext rdbParseContext;

    private STATE state = STATE.READ_MAGIC;

    private CompositeByteBuf temp;

    private byte magicReadIndex;

    private RdbParseContext.RdbType currentType;

    private short rdbVersion;

    private static final Logger logger = LoggerFactory.getLogger(DefaultRdbParser.class);

    enum STATE {
        READ_INIT,
        READ_MAGIC,
        READ_VERSION,
        READ_TYPE,
        READ_CONTENT,
        READ_END
    }

    public DefaultRdbParser() {
        this(new DefaultRdbParseContext());
    }

    public DefaultRdbParser(RdbParseContext parserManager) {
        this.rdbParseContext = parserManager;
    }

    @Override
    public void read(ByteBuf byteBuf) {

        while (byteBuf.readableBytes() > 0) {

            switch (state) {
                case READ_INIT:
                    temp = null;
                    magicReadIndex = 0;
                    currentType = null;
                    state = STATE.READ_MAGIC;
                    break;

                case READ_MAGIC:
                    magicReadIndex += checkMagic(byteBuf, magicReadIndex);
                    if (magicReadIndex >= RdbConstant.REDIS_RDB_MAGIC.length) {
                        state = STATE.READ_VERSION;
                    }
                    break;

                case READ_VERSION:
                    if (null == temp && byteBuf.readableBytes() >= 4) {
                        rdbVersion = Short.parseShort(byteBuf.toString(byteBuf.readerIndex(), 4, Codec.defaultCharset));
                        state = STATE.READ_TYPE;
                    } else if (null == temp) {
                        temp = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(4);
                        temp.addComponent(true, byteBuf.readBytes(byteBuf.readableBytes()));
                    } else {
                        int readCnt = Math.min(byteBuf.readableBytes(), 4 - temp.readableBytes());
                        temp.addComponent(true, byteBuf.readBytes(readCnt));
                        if (temp.readableBytes() == 4) {
                            rdbVersion = Short.parseShort(temp.toString(Codec.defaultCharset));
                            temp = null;
                            state = STATE.READ_VERSION;
                        }
                    }
                    break;

                case READ_TYPE:
                    if (0 == byteBuf.readableBytes()) break;
                    char type = byteBuf.readChar();
                    currentType = RdbParseContext.RdbType.findByCode(type);
                    if (null == currentType) {
                        throw new XpipeRuntimeException("unknown " + (short)type);
                    } else if (currentType.equals(RdbParseContext.RdbType.EOF)) {
                        state = STATE.READ_END;
                        notifyFinish();
                    } else {
                        state = STATE.READ_CONTENT;
                    }
                    break;

                case READ_CONTENT:
                    RdbParser subParser = rdbParseContext.getOrCreateParser(currentType);
                    if (null == subParser) {
                        throw new XpipeRuntimeException("no parser for type " + currentType);
                    }
                    subParser.read(byteBuf);
                    if (subParser.isFinish()) {
                        subParser.reset();
                        state = STATE.READ_TYPE;
                        currentType = null;
                    }
                    break;

                case READ_END:
                default:
                    // do nothing
            }
        }
    }

    private int checkMagic(ByteBuf byteBuf, int checkIdx) {
        int readCnt = 0;
        while (checkIdx < RdbConstant.REDIS_RDB_MAGIC.length && byteBuf.readableBytes() > 0) {
            char current = byteBuf.readChar();
            if (RdbConstant.REDIS_RDB_MAGIC[checkIdx + readCnt] != current) {
                throw new XpipeRuntimeException("unexpected rdb magic " + current + " at " + checkIdx + readCnt);
            }
            readCnt++;
        }

        return readCnt;
    }

    public short getRdbVersion() {
        return rdbVersion;
    }

    @Override
    public void registerListener(RdbParseListener listener) {
        rdbParseContext.registerListener(listener);
    }

    @Override
    public void unregisterListener(RdbParseListener listener) {
        rdbParseContext.unregisterListener(listener);
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        this.state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
