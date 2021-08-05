package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.StringInOutPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommandBulkStringParser extends AbstractBulkStringParser {

    private COMMAND_STATS commandStats = COMMAND_STATS.READING_CONTENT;

    private static Logger logger = LoggerFactory.getLogger(CommandBulkStringParser.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    public enum COMMAND_STATS{
        READING_CONTENT,
        READING_CR,
        READING_LF,
        END
    }

    public CommandBulkStringParser(String content) {
        super(new StringInOutPayload(content));
    }

    public CommandBulkStringParser(InOutPayload payload) {
        super(payload);
    }

    protected  boolean byteMustEqualIfExists(ByteBuf byteBuf, char eqbyte) {
        if (byteBuf.readableBytes() == 0) {
            return false;
        }
        byte data1 = byteBuf.getByte(byteBuf.readerIndex());
        if (data1 == eqbyte) {
            byteBuf.readByte();
            return true;
        } else {
            throw new RedisRuntimeException(String.format("Parse the Redis command protocol Error: Here should be '%s' ,but it's %s", eqbyte, (char)data1));
        }
    }

    protected boolean readEofJudger(ByteBuf byteBuf) {
        boolean result = super.readEofJudger(byteBuf);
        if(result) {
            if(!(eofJudger instanceof AbstractBulkStringEoFJudger.BulkStringLengthEofJudger)) {
                throw new RedisRuntimeException("redis command parsing string protocol error");
            }
        }
        return result;
    }

    @Override
    boolean readContent(ByteBuf byteBuf) {
        while(true) {
            switch (commandStats) {
                case READING_CONTENT:
                    if(!readPayload(byteBuf)) { return false; }
                    commandStats = COMMAND_STATS.READING_CR;
                case READING_CR:
                    if(!byteMustEqualIfExists(byteBuf, '\r')) { return false; }
                    commandStats = COMMAND_STATS.READING_LF;
                case READING_LF:
                    if(!byteMustEqualIfExists(byteBuf, '\n')) { return false; }
                    commandStats = COMMAND_STATS.END;
                case END:
                    return true;
            }
        }
    }

    @Override
    RedisClientProtocol<InOutPayload> getResult() {
        return new CommandBulkStringParser(payload);
    }


}
