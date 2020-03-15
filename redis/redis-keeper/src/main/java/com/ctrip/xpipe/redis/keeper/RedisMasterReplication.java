package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.keeper.ratelimit.PsyncChecker;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public interface RedisMasterReplication extends PsyncChecker, Lifecycle{

	void handleResponse(Channel channel, ByteBuf msg) throws XpipeException;

	void masterDisconntected(Channel channel);

	void masterConnected(Channel channel);
	
	PARTIAL_STATE partialState();
	
	RedisMaster redisMaster();

	void updateReplicationObserver(RedisMasterReplicationObserver observer);

	public interface RedisMasterReplicationObserver extends PsyncChecker {

		void onMasterConnected();

		void onMasterDisconnected();

		void onDumpFinished();

		void onDumpFail();
	}

}
