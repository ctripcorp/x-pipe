package com.ctrip.xpipe.redis.core.protocal.protocal;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Dec 22, 2016
 */
public interface BulkStringEofJudger {
	
	JudgeResult end(ByteBuf byteBuf);
	
	long expectedLength();
	
	int truncate();
	
	EofType getEofType();
	
	public static class JudgeResult{
		
		private boolean end;
		private int readLen;
		
		public JudgeResult(boolean end, int readLen){
			this.end = end;
			this.readLen = readLen;
		}
		
		public boolean isEnd() {
			return end;
		}
		
		public int getReadLen() {
			return readLen;
		}
		
	}
	
}
