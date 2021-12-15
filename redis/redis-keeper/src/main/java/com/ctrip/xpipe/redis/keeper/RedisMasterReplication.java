package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.keeper.ratelimit.PsyncChecker;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public interface RedisMasterReplication extends PsyncChecker, Lifecycle{

	Endpoint masterEndpoint();

	void handleResponse(Channel channel, ByteBuf msg) throws XpipeException;

	void masterDisconnected(Channel channel);

	void masterConnected(Channel channel);
	
	PARTIAL_STATE partialState();
	
	RedisMaster redisMaster();

	void reconnectMaster();

	CommandFuture<Void> waitReplConnected();

	CommandFuture<Void> waitReplStopCompletely();

	boolean isReplStopCompletely();

	void updateReplicationObserver(RedisMasterReplicationObserver observer);

	public interface RedisMasterReplicationObserver extends PsyncChecker {

		void onMasterConnected();

		void onMasterDisconnected();

		void onDumpFinished();

		void onDumpFail(Throwable th);
	}

}
