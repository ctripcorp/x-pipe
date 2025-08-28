package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 * Dec 22, 2016
 */
public abstract class AbstractBulkStringEoFJudger implements BulkStringEofJudger{

	private static final Logger log = LoggerFactory.getLogger(AbstractBulkStringEoFJudger.class);
	protected AtomicLong realLen = new AtomicLong();

	@Override
	public JudgeResult end(ByteBuf byteBuf) {
		
		realLen.addAndGet(byteBuf.readableBytes());
		return doEnd(byteBuf);
	}
	protected abstract JudgeResult doEnd(ByteBuf byteBuf);


	public static class BulkStringLengthEofJudger extends AbstractBulkStringEoFJudger{
		
		private final long expectedLen;
		
		public BulkStringLengthEofJudger(long expectedLen) {
			if(expectedLen < 0){
				throw new RedisRuntimeException("expectedLen < 0:" + expectedLen);
			}
			this.expectedLen = expectedLen;
		}

		@Override
		public JudgeResult doEnd(ByteBuf byteBuf) {
			
			int readable = byteBuf.readableBytes();
			long currentLen = realLen.get();
			if(currentLen >= expectedLen){
				int read = (int) (expectedLen - (currentLen - readable));
				if(read < 0){
					throw new IllegalStateException("readlen < 0:" + read);
				}
				return new JudgeResult(true, read);
			}else{
				return new JudgeResult(false, readable);
			}
		}

		@Override
		public long expectedLength() {
			return expectedLen;
		}
		
		@Override
		public String toString() {
			return String.format("expectedLen:%d, readLen:%d", expectedLen, realLen.get());
		}

		@Override
		public int truncate() {
			return 0;
		}

		@Override
		public EofType getEofType() {
			return new LenEofType(expectedLen);
		}
		
	}
	
	
	public static class BulkStringEofMarkJudger extends AbstractBulkStringEoFJudger{
		
		public static final int MARK_LENGTH = RedisClientProtocol.RUN_ID_LENGTH;
		
		private final byte[] eofmark;
		
		private volatile boolean alreadyFinished = false;
		
		private byte lastData[] = new byte[MARK_LENGTH];
		
		public BulkStringEofMarkJudger(byte[] eofmark) {
			this.eofmark = eofmark;
		}

		@Override
		public JudgeResult doEnd(ByteBuf raw) {
			
			if(alreadyFinished){
				throw new IllegalStateException("doEnd already ended:" + this);
			}
			
			ByteBuf used = raw.slice();
			int readable = used.readableBytes();
			
			if(readable > MARK_LENGTH){
				int writeIndex = used.writerIndex();
				used.readerIndex(writeIndex - MARK_LENGTH);
			}
			
			int dataLen = used.readableBytes();
			int remLen = MARK_LENGTH - dataLen;
			
			System.arraycopy(lastData, dataLen, lastData, 0, remLen);
			used.readBytes(lastData, remLen, dataLen);
			boolean ends = Arrays.equals(eofmark, lastData);
			if(ends){
				alreadyFinished = true;
			}
			return new JudgeResult(ends, readable);
		}

		public byte[] getLastData() {
			return lastData;
		}

		@Override
		public int truncate() {
			return MARK_LENGTH;
		}

		@Override
		public long expectedLength() {
			return -1;
		}
		
		@Override
		public String toString() {
			return String.format("eofmark:%s, realLen:%d", new String(eofmark), realLen.get());
		}

		@Override
		public EofType getEofType() {
			return new EofMarkType(new String(eofmark));
		}

	}
}
