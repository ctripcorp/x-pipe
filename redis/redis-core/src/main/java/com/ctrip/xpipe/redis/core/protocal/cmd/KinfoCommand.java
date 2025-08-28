package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author marsqing
 *
 *         Jun 1, 2016 11:05:11 AM
 */
public class KinfoCommand extends AbstractRedisCommand<ReplicationStoreMeta> {

	private String args;
	public KinfoCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		this(clientPool, "", scheduled);
	}

	public KinfoCommand(SimpleObjectPool<NettyClient> clientPool, String args, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
		this.args = args;
	}

	@Override
	public String getName() {
		return "kinfo";
	}

	@Override
	public ByteBuf getRequest() {
		
		RequestStringParser requestString = new RequestStringParser(getName(), args);
		return requestString.format();
	}
	
	
	
	protected ReplicationStoreMeta format(Object payload) {

		ByteArrayOutputStreamPayload data = (ByteArrayOutputStreamPayload) payload;
		String buff = new String(data.getBytes(), Codec.defaultCharset);
		
		getLogger().info("[format]{}", buff);
		
		ReplicationStoreMeta meta = null;
		meta = Codec.DEFAULT.decode(buff, ReplicationStoreMeta.class);
		if(valid(meta)){
			return meta;
		}else{
			throw new XpipeRuntimeException("[format][wrong meta]" + meta);
		}
	}

	private boolean valid(ReplicationStoreMeta meta) {
		if(meta == null){
			return false;
		}
		//TODO kinfo support replid, beginoffset
		if(meta.getReplId() == null || meta.getReplId().length() != RedisProtocol.RUN_ID_LENGTH){
			return false;
		}
		if(meta.getBeginOffset() == null){
			return false;
		}
		if(meta.getMasterAddress() == null){
			return false;
		}
		
		return true;
	};
}
