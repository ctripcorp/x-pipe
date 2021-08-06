package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.StringInOutPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;

public class CommandBulkStringParser extends AbstractBulkStringParser {

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
                byte data1 = byteBuf.getByte(byteBuf.readerIndex());
                if (data1 == '\r') {
                    byteBuf.readByte();
                    commandState = COMMAND_STATE.READING_LF;
                } else {
                    throw new RedisRuntimeException(String.format("command eof not '\r': %s", data1));
                }
            case READING_LF:
                if (byteBuf.readableBytes() == 0) {
                    return null;
                }
                data1 = byteBuf.getByte(byteBuf.readerIndex());
                if (data1 == '\n') {
                    byteBuf.readByte();
                    commandState = COMMAND_STATE.END;
                } else {
                    throw new RedisRuntimeException(String.format("command eof not '\n': %s", data1));
                }
            case END:
                return new CommandBulkStringParser(payload);
            default:
                return null;
        }
    }
}
