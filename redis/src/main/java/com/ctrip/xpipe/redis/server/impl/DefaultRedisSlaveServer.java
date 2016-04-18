package com.ctrip.xpipe.redis.server.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.Command;
import com.ctrip.xpipe.redis.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.server.RedisSlaveServer;
import com.ctrip.xpipe.thread.NamedThreadFactory;
import com.ctrip.xpipe.utils.CpuUtils;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:08:26
 */
public class DefaultRedisSlaveServer extends AbstractRedisServer implements RedisSlaveServer{
	
	public static final int REPLCONF_INTERVAL_MILLI = 1000;

	private Endpoint endpoint;
	private String masterRunId;
	private long masterBeginOffset;
	
	private long reploff;
	
	private OutputStream commandOus;
	private InOutPayload rdbPayload;
	

	private InputStream socketIns;
	private OutputStream socketOus;
	private Socket socket = null;
	
	@SuppressWarnings("unused")
	private int retry = 3;
	private int slavePort;

	private ScheduledExecutorService scheduled;
	
	public DefaultRedisSlaveServer(Endpoint endpoint, InOutPayload rdbPayload, OutputStream commandOus, int slavePort) {
		this(endpoint, rdbPayload, commandOus, "?", -1L, slavePort, null, 3);
	}
	
	public DefaultRedisSlaveServer(Endpoint endpoint, InOutPayload rdbPayload, OutputStream commandOus, String masterRunId, long masterBeginOffset, int slavePort, ScheduledExecutorService scheduled, int retry) {

		this.endpoint = endpoint;
		this.rdbPayload = rdbPayload;
		this.commandOus = commandOus;
		this.masterRunId = masterRunId;
		setMasterBeginOffset(masterBeginOffset);
		this.retry = retry;
		this.slavePort = slavePort;
		this.scheduled = scheduled;
		if(scheduled == null){
			this.scheduled = Executors.newScheduledThreadPool(CpuUtils.getCpuCount(), new NamedThreadFactory(endpoint.toString()));
		}
	}

	private void psync() throws IOException, XpipeException {
		
		Psync psync = new Psync(socketOus, socketIns, masterRunId, masterBeginOffset, rdbPayload, commandOus);
		psync.addPsyncObserver(this);
		psync.request();
	}

	private void connect() throws UnknownHostException, IOException {
		
		socket = new Socket(endpoint.getHost(), endpoint.getPort());
		socketOus = socket.getOutputStream();
		socketIns = socket.getInputStream();
		
	}


	private void sendListeningPort() throws IOException, XpipeException{
		
		Command replconf = new Replconf(ReplConfType.LISTENING_PORT, String.valueOf(slavePort), socketOus, socketIns);
		replconf.request();		
	}
	
	@Override
	protected void doRun() {
		try{
			connect();
			sendListeningPort();
			psync();
		}catch(Exception e){
			logger.error("[start]" + endpoint, e);
		}
	}

	private void scheduleReplconf() {
		
		if(logger.isInfoEnabled()){
			logger.info("[scheduleReplconf]" + this);
		}
		
		scheduled.scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				try{
					Command command = new Replconf(ReplConfType.ACK, String.valueOf(reploff), socketOus, socketIns);
					command.request();
				}catch(Throwable th){
					logger.error("[run][send replack error]" + DefaultRedisSlaveServer.this, th);
				}
			}
		}, REPLCONF_INTERVAL_MILLI, REPLCONF_INTERVAL_MILLI, TimeUnit.MILLISECONDS);
	}

	@Override
	public void setFullSyncInfo(String masterRunId, long masterBeginOffset) {
		
		if(!this.masterRunId.equals(masterRunId)){
			
			if(logger.isInfoEnabled()){
				logger.info("[setFullSyncInfo][masterRunIdChanged]" + this.masterRunId + " -> " + masterRunId);
			}
			this.masterRunId = masterRunId;
		}
		if(this.masterBeginOffset != masterBeginOffset){
			if(logger.isInfoEnabled()){
				logger.info("[setFullSyncInfo][masterOffsetChanged]" + this.masterBeginOffset + " -> " + masterBeginOffset);
			}
			setMasterBeginOffset(masterBeginOffset);
		}
	}

	private void setMasterBeginOffset(long masterBeginOffset) {
		
		this.masterBeginOffset = masterBeginOffset;
		this.reploff = masterBeginOffset;
	}

	@Override
	public void increaseReploffset() {
		reploff++;
	}

	@Override
	public void increaseReploffset(int n) {
		reploff += n;
	}

	@Override
	public long getReploffset() {
		return reploff;
	}
	
	@Override
	public String toString() {
		return "endpoint:" + this.endpoint + ",masterRunId:" + this.masterRunId + ",offset:" + this.reploff;
	}

	@Override
	public void beginWriteRdb() {
		
	}

	@Override
	public void endWriteRdb() {
		scheduleReplconf();
	}
}
