package com.ctrip.xpipe.redis.core.protocal.cmd;



import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;

import io.netty.buffer.ByteBuf;

/**
 * @author marsqing
 *
 *         Jun 1, 2016 11:05:11 AM
 */
public class KinfoCommand extends AbstractRedisCommand<ReplicationStoreMeta> {

	private String args;
	public KinfoCommand(SimpleObjectPool<NettyClient> clientPool) {
		this(clientPool, "");
	}

	public KinfoCommand(SimpleObjectPool<NettyClient> clientPool, String args) {
		super(clientPool);
		this.args = args;
	}

	@Override
	public String getName() {
		return "kinfo";
	}

	@Override
	protected ByteBuf getRequest() {
		
		RequestStringParser requestString = new RequestStringParser(getName(), args);
		return requestString.format();
	}
	
	
	
	protected ReplicationStoreMeta format(Object payload) {

		ByteArrayOutputStreamPayload data = (ByteArrayOutputStreamPayload) payload;
		String buff = new String(data.getBytes(), Codec.defaultCharset);
		
		logger.info("[onComplete]{}", buff);
		
		ReplicationStoreMeta meta = null;
		try{
			meta = JSON.parseObject(buff, ReplicationStoreMeta.class);
			if(meta != null && meta.getMasterRunid() != null && meta.getMasterRunid().length() == RedisProtocol.RUN_ID_LENGTH){
				return meta;
			}else{
				throw new XpipeRuntimeException("[format][wrong meta]" + meta);
			}
		}catch(Exception e1){
			logger.error("[onComplete]" + getName() + "," + buff, e1);
		}
		
		return meta;
	};
}
