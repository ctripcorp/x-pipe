package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.AbstractBulkStringEoFJudger.BulkStringEofMarkJudger;
import com.ctrip.xpipe.redis.core.protocal.protocal.AbstractBulkStringEoFJudger.BulkStringLengthEofJudger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Dec 23, 2016
 */
public class BulkStringEofJuderManager {
	
	private static Logger logger = LoggerFactory.getLogger(BulkStringEofJuderManager.class);
	
	public static BulkStringEofJudger create(byte []data){
		
		int start = 0;
		if(data[0] == RedisClientProtocol.DOLLAR_BYTE){
			start = 1;
		}
		
		if(arraynequals(data, 1, RedisClientProtocol.EOF, RedisClientProtocol.EOF.length)){
			if(start + RedisClientProtocol.EOF.length + BulkStringEofMarkJudger.MARK_LENGTH > data.length){
				throw new IllegalStateException("bulksting eof mark error:" + new String(data));
			}
			byte []mark = new byte[BulkStringEofMarkJudger.MARK_LENGTH];
			System.arraycopy(data, start + RedisClientProtocol.EOF.length, mark, 0, BulkStringEofMarkJudger.MARK_LENGTH);
			return new BulkStringEofMarkJudger(mark);
		}
		
		logger.debug("[create]len:{}, {}, {}", data.length, new String(data), start);
		String lengthStr = new String(data, start, data.length - start).trim();
		
		long length = Long.parseLong(lengthStr);
		return new BulkStringLengthEofJudger(length);
	}

	private static boolean arraynequals(byte[] data, int index, byte[] expected, int length) {
		
		if(data.length < index + length){
			return false;
		}
		if(expected.length < length){
			return false;
		}
		
		for(int i=0;i<length;i++){
			if(data[index+i] != expected[i]){
				return false;
			}
		}
		return true;
	}

}
