package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;


/**
 *  $EOF:{bytes[40]}\r\n
 *  {rdb}
 *  {bytes[40]}
 *
 *
 *  ${rdbsize}\r\n
 *  rdb
 */
public class RdbBulkStringParser extends AbstractBulkStringParser {
    public RdbBulkStringParser(String content) {
        super(content);
    }

    public RdbBulkStringParser(InOutPayload bulkStringPayload) {
        super(bulkStringPayload);
    }
    @Override
    public RedisClientProtocol<InOutPayload> read(ByteBuf byteBuf) {
        while(true) {
            switch (bulkStringState) {
                case READING_EOF_MARK:
                    /*
                        1.  $EOF:<40 bytes unguessable hex string>\r\n
                            <rdb>
                            <40 bytes unguessable hex string>
                        2. $<rdb file size>\r\n
                            <rdb>
                     */
                    BulkStringEofJudger eofJudger = readEOfMark(byteBuf);
                    if (eofJudger == null) {
                        return null;
                    }
                    setEofJudger(eofJudger);
                    startInput();
                    bulkStringState = BulkStringParser.BULK_STRING_STATE.READING_CONTENT;
                case READING_CONTENT:
                    BulkStringEofJudger.JudgeResult result = addContext(byteBuf);
                    if(result.isEnd()) {
                        endInput();
                        bulkStringState = BulkStringParser.BULK_STRING_STATE.END;
                        continue;
                    }
                    break;
                case END:
                    return new RdbBulkStringParser(payload);
                default:
                    throw new RedisRuntimeException(String.format("read RdbBulkStringParser error %s", bulkStringState.toString()));
            }
            return null;
        }
    }
}

