package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;

public class CommandBulkStringParaser extends AbstractBulkStringParser {
    public CommandBulkStringParaser(String content) {
        super(content);
    }

    public CommandBulkStringParaser(InOutPayload bulkStringPayload) {
        super(bulkStringPayload);
    }

    @Override
    public RedisClientProtocol<InOutPayload> read(ByteBuf byteBuf) {
        while(true) {
            switch (bulkStringState) {
                case READING_EOF_MARK:
                    BulkStringEofJudger eofJudger = readEOfMark(byteBuf);
                    if (eofJudger == null) {
                        return null;
                    }
                    setEofJudger(eofJudger);
                    bulkStringState = BulkStringParser.BULK_STRING_STATE.READING_CONTENT;
                case READING_CONTENT:
                    BulkStringEofJudger.JudgeResult result = addContext(byteBuf);
                    if(result.isEnd()) {
                        endInput();
                        bulkStringState = BulkStringParser.BULK_STRING_STATE.READING_CR;
                        continue;
                    } else {
                        break;
                    }
                case READING_CR:
                    if(!cmpCharUpdateState(byteBuf, '\r', BulkStringParser.BULK_STRING_STATE.READING_LF)) {
                        return null;
                    }
                    continue;
                case READING_LF:
                    if(!cmpCharUpdateState(byteBuf, '\n', BulkStringParser.BULK_STRING_STATE.END)) {
                        return null;
                    }
                    continue;
                case END:
                    return new CommandBulkStringParaser(payload);
                default:
                    throw new RedisRuntimeException(String.format("read bulkStringState error %s", bulkStringState.toString()));
            }
            return null;
        }
    }
}