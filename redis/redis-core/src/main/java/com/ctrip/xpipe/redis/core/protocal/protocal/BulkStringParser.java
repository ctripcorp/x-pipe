package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannel;
import com.ctrip.xpipe.payload.StringInOutPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringEofJudger.JudgeResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:35:36
 */
public abstract class BulkStringParser extends AbstractRedisClientProtocol<InOutPayload> {
	public enum BULK_STRING_STATE{
		READING_EOF_MARK,
		READING_CONTENT,
		READING_CR,
		READING_LF,
		END
	}
	public static interface BulkStringParserListener{
		void onEofType(EofType eofType);
	}

	public BulkStringParser(InOutPayload bulkStringPayload) {
		super(bulkStringPayload, false, false);
	}

	protected BulkStringParser.BulkStringParserListener bulkStringParserListener;

	public void setBulkStringParserListener(BulkStringParser.BulkStringParserListener bulkStringParserListener) {
		this.bulkStringParserListener = bulkStringParserListener;
	}

}