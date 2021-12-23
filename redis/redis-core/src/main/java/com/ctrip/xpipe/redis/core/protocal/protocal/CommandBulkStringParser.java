package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.StringInOutPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandBulkStringParser extends AbstractBulkStringParser {
    
    private static final Logger logger = LoggerFactory.getLogger(CommandBulkStringParser.class);
    
    @Override
    protected Logger getLogger() {
        return logger;
    }
    
    private COMMAND_STATE commandState = COMMAND_STATE.READING_CR;

    private enum COMMAND_STATE {
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

    @Override
    protected RedisClientProtocol<InOutPayload> readEnd(ByteBuf byteBuf) {
        switch(commandState) {
            case READING_CR:
                if (byteBuf.readableBytes() == 0) {
                    return null;
                }
                byte data1 = byteBuf.readByte();
                if (data1 == '\r') {
                    commandState = COMMAND_STATE.READING_LF;
                } else {
                    throw new RedisRuntimeException(String.format("command eof not '\\r', but: %d, %c", data1, data1));
                }
            case READING_LF:
                if (byteBuf.readableBytes() == 0) {
                    return null;
                }
                data1 = byteBuf.readByte();
                if (data1 == '\n') {
                    commandState = COMMAND_STATE.END;
                } else {
                    throw new RedisRuntimeException(String.format("command eof not '\\r', but: %d, %c", data1, data1));
                }
            case END:
                return new CommandBulkStringParser(payload);
            default:
                return null;
        }
    }
}
