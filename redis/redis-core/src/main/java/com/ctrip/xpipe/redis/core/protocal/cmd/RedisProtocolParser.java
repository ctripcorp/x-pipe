package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.payload.InOutPayloadFactory;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.*;
import io.netty.buffer.ByteBuf;

public class RedisProtocolParser {

    private enum PARSE_STATE {
        READING_SIGN,
        READING_CONTENT
    }

    private PARSE_STATE currentState = PARSE_STATE.READING_SIGN;
    private RedisClientProtocol<?> redisClientProtocol;
    private InOutPayloadFactory inOutPayloadFactory;
    protected Long currentCommandOffset;

    public RedisProtocolParser() {
        // Default constructor
        currentCommandOffset = 0L;
    }

    public RedisProtocolParser(InOutPayloadFactory inOutPayloadFactory) {
        this.inOutPayloadFactory = inOutPayloadFactory;
        currentCommandOffset = 0L;
    }

    public Object parse(ByteBuf byteBuf) {
        int pre = byteBuf.readerIndex();
        try {
            if (currentState == PARSE_STATE.READING_SIGN) {
                if (!tryInitializeProtocol(byteBuf)) {
                    return null; // Not enough data to read the sign byte
                }
            }

            if (currentState == PARSE_STATE.READING_CONTENT) {
                RedisClientProtocol<?> result = redisClientProtocol.read(byteBuf);
                if (result != null) {
                    // Parsing is complete for this response
                    return result.getPayload();
                }
            }

            return null; // More data is needed
        } finally {
            currentCommandOffset += byteBuf.readerIndex() - pre;
        }
    }

    private boolean tryInitializeProtocol(ByteBuf byteBuf) {

        while (redisClientProtocol == null && byteBuf.readableBytes() > 0) {
            byte sign = byteBuf.readByte();
            switch (sign) {
                case '\r':
                case '\n':
                    // Skip CRLF, common between pipelined commands
                    continue;
                case RedisClientProtocol.MINUS_BYTE:
                    redisClientProtocol = new RedisErrorParser();
                    break;
                case RedisClientProtocol.ASTERISK_BYTE:
                    // Propagate the payload factory to the array parser
                    redisClientProtocol = new ArrayParser().setInOutPayloadFactory(inOutPayloadFactory);
                    break;
                case RedisClientProtocol.DOLLAR_BYTE:
                    redisClientProtocol = createBulkStringParser();
                    break;
                case RedisClientProtocol.COLON_BYTE:
                    redisClientProtocol = new LongParser();
                    break;
                case RedisClientProtocol.PLUS_BYTE:
                    redisClientProtocol = new SimpleStringParser();
                    break;
                default:
                    throw new RedisRuntimeException("Unknown protocol sign: " + (char) sign);
            }
        }

        if (redisClientProtocol != null) {
            currentState = PARSE_STATE.READING_CONTENT;
            return true;
        }

        return false;
    }

    private CommandBulkStringParser createBulkStringParser() {
        InOutPayload payload;
        if (inOutPayloadFactory != null) {
            payload = inOutPayloadFactory.create();
        } else {
            // Default payload type if no factory is provided
            payload = new ByteArrayOutputStreamPayload();
        }
        return new CommandBulkStringParser(payload);
    }

    /**
     * Resets the parser to its initial state, allowing it to be reused for a new response.
     */
    public void reset() {
        currentState = PARSE_STATE.READING_SIGN;
        redisClientProtocol = null;
        currentCommandOffset = 0L;
    }

    public Long getCurrentCommandOffset() {
        return currentCommandOffset;
    }

}
