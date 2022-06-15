package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * @author lishanglin
 * date 2022/5/28
 */
public class DefaultRdbParser extends AbstractRdbParser<Void> implements RdbParser<Void> {

    private RdbParseContext rdbParseContext;

    protected STATE state = STATE.READ_INIT;

    private ByteBuf temp;

    private byte magicReadIndex;

    private RdbParseContext.RdbType currentType;

    private short rdbVersion;

    private static final Logger logger = LoggerFactory.getLogger(DefaultRdbParser.class);

    protected enum STATE {
        READ_INIT,
        READ_MAGIC,
        READ_VERSION,
        READ_TYPE,
        READ_OP,
        READ_KEY,
        READ_VAL_CONTENT,
        READ_END
    }

    public DefaultRdbParser() {
        this(new DefaultRdbParseContext());
    }

    public DefaultRdbParser(RdbParseContext parserManager) {
        this.rdbParseContext = parserManager;
        this.rdbParseContext.bindRdbParser(this);
    }

    @Override
    public Void read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

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
                    temp = readUntilBytesEnough(byteBuf, temp, 4);
                    if (temp.readableBytes() == 4) {
                        rdbVersion = Short.parseShort(temp.toString(StandardCharsets.US_ASCII));
                        temp.release();
                        temp = null;
                        state = STATE.READ_TYPE;
                    }
                    break;

                case READ_TYPE:
                    short type = byteBuf.readUnsignedByte();
                    currentType = RdbParseContext.RdbType.findByCode(type);

                    getLogger().debug("[read][type] {}", currentType);
                    if (null == currentType) {
                        throw new XpipeRuntimeException("unknown rdb type:" + type);
                    } else if (currentType.equals(RdbParseContext.RdbType.EOF)) {
                        state = STATE.READ_END;
                    } else if (currentType.isRdbOp()) {
                        state = STATE.READ_OP;
                    } else {
                        state = STATE.READ_KEY;
                    }
                    break;

                case READ_OP:
                    RdbParser<?> subOpParser = rdbParseContext.getOrCreateParser(currentType);
                    if (null == subOpParser) {
                        throw new XpipeRuntimeException("no parser for type " + currentType);
                    }
                    subOpParser.read(byteBuf);
                    if (subOpParser.isFinish()) {
                        subOpParser.reset();
                        state = STATE.READ_TYPE;
                    }
                    break;

                case READ_KEY:
                    RdbParser<byte[]> subKeyParser = (RdbParser<byte[]>) rdbParseContext.getOrCreateParser(currentType);
                    if (null == subKeyParser) {
                        throw new XpipeRuntimeException("no parser for type " + currentType);
                    }
                    byte[] key = subKeyParser.read(byteBuf);
                    if (null != key) {
                        subKeyParser.reset();
                        rdbParseContext.setKey(new RedisKey(key));
                        state = STATE.READ_VAL_CONTENT;
                    }
                    break;

                case READ_VAL_CONTENT:
                    RdbParser<?> subValParser = rdbParseContext.getOrCreateParser(currentType);
                    if (null == subValParser) {
                        throw new XpipeRuntimeException("no parser for type " + currentType);
                    }
                    subValParser.read(byteBuf);
                    if (subValParser.isFinish()) {
                        subValParser.reset();
                        rdbParseContext.clearKvContext();
                        state = STATE.READ_TYPE;
                    }
                    break;

                case READ_END:
                default:
            }

            if (isFinish()) {
                notifyFinish();
            }
        }

        return null;
    }

    private int checkMagic(ByteBuf byteBuf, int checkIdx) {
        int readCnt = 0;
        while (checkIdx + readCnt < RdbConstant.REDIS_RDB_MAGIC.length && byteBuf.readableBytes() > 0) {
            char current = (char)byteBuf.readByte(); // ascii to char
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
        super.registerListener(listener);
        rdbParseContext.registerListener(listener);
    }

    @Override
    public void unregisterListener(RdbParseListener listener) {
        super.unregisterListener(listener);
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
