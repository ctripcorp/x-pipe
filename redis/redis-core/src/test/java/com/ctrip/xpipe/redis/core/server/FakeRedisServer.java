package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;

import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public class FakeRedisServer extends AbstractLifecycle{
	
	private int rdbSize = 100;
	private int commandsLength = 1000;
	private int sendBatchSize = 100;
	private int sendBatchIntervalMilli = 10;
	
	private int port;
	private String rdbContent = "";
	private String commands = "";
	private int    rdbOffset = 1;
	private Server server; 
	private String runId = RunidGenerator.DEFAULT.generateRunid();
	
	private boolean eof = Boolean.parseBoolean(System.getProperty("EOF", "true"));  
	
	private int sleepBeforeSendFullSyncInfo = 0;
	private int sleepBeforeSendRdb = 0;
	private boolean sendLFBeforeSendRdb = true;
	private AtomicInteger sendHalfRdbAndCloseConnectionCount = new AtomicInteger(0);
	
	private List<FakeRedisServerAction> commandListeners = new LinkedList<>();

	//for statis
	private AtomicInteger psyncCount = new AtomicInteger(0);

	private boolean isKeeper = false;
	private boolean partialSyncFail = false;

	public FakeRedisServer(int port){
		this(port, 0);
	}

	public FakeRedisServer(int port, int sleepBeforeSendRdb){
		
		this.port = port;
		this.sleepBeforeSendRdb = sleepBeforeSendRdb;
		this.server = new Server(port, new IoActionFactory() {
			
			@Override
			public IoAction createIoAction(Socket socket) {
				return new FakeRedisServerAction(FakeRedisServer.this, socket);
			}
		});
	}

	public void setIsKeeper(boolean isKeeper){
		this.isKeeper = isKeeper;
	}

	public boolean isKeeper() {
		return isKeeper;
	}

	public int getPsyncCount() {
		return psyncCount.get();
	}

	public int getConnected() {
		return server.getConnected();
	}




	public int getPort() {
		return port;
	}

	public String getRdbContent() {
		return rdbContent;
	}

	public int getRdbOffset() {
		return rdbOffset;
	}
	
	public String getRunId() {
		return runId;
	}
	
	public int getSleepBeforeSendRdb() {
		return sleepBeforeSendRdb;
	}

	public synchronized void reGenerateRdb() {

		rdbOffset += commands.length();
		
		String prefix = String.format("rdb_rdboffset:%d--", rdbOffset);
		rdbContent = prefix + AbstractTest.randomString(rdbSize - prefix.length());

		if (commandsLength < 0) return;
		prefix = String.format("cmd_rdboffset:%d--", rdbOffset);
		commands = prefix + AbstractTest.randomString(commandsLength - prefix.length());
		addCommands(commands);
	}

	public void propagate(String cmd) {
		addCommands(cmd);
	}
	
	private void addCommands(String commands) {
		for(FakeRedisServerAction listener :  commandListeners){
			listener.addCommands(commands);
		}
	}
	public int getCommandsLength() {
		return commandsLength;
	}

	public void setCommandsLength(int commandsLength) {
		this.commandsLength = commandsLength;
	}
	
	public int getRdbSize() {
		return rdbSize;
	}

	public void addCommandsListener(FakeRedisServerAction fakeRedisServerAction) {
		
		logger.debug("[addCommandsListener]{}", fakeRedisServerAction);
		fakeRedisServerAction.addCommands(commands);
		commandListeners.add(fakeRedisServerAction);
	}


	public String currentCommands() {
		return commands;
	}
	
	public int getSendBatchSize() {
		return sendBatchSize;
	}
	
	public int getSendBatchIntervalMilli() {
		return sendBatchIntervalMilli;
	}
	
	public void setSleepBeforeSendRdb(int sleepBeforeSendRdb) {
		this.sleepBeforeSendRdb = sleepBeforeSendRdb;
	}

	public void removeListener(FakeRedisServerAction fakeRedisServerAction) {
		logger.debug("[removeListener]{}", fakeRedisServerAction);
		commandListeners.remove(fakeRedisServerAction);
	}
	
	
	public static void main(String []argc) throws Exception{
		
		FakeRedisServer fakeRedisServer = new FakeRedisServer(1234);
		fakeRedisServer.initialize();
		fakeRedisServer.start();
	}
	
	public Server getServer() {
		return server;
	}

	public int getSleepBeforeSendFullSyncInfo() {
		return sleepBeforeSendFullSyncInfo;
	}
	
	public void setSleepBeforeSendFullSyncInfo(int sleepBeforeSendFullSyncInfo) {
		this.sleepBeforeSendFullSyncInfo = sleepBeforeSendFullSyncInfo;
	}

	public boolean isSendLFBeforeSendRdb() {
		return sendLFBeforeSendRdb;
	}

	public void setSendLFBeforeSendRdb(boolean sendLFBeforeSendRdb) {
		this.sendLFBeforeSendRdb = sendLFBeforeSendRdb;
	}
	
	public void setSendHalfRdbAndCloseConnectionCount(int sendHalfRdbAndCloseConnectionCount) {
		this.sendHalfRdbAndCloseConnectionCount.set(sendHalfRdbAndCloseConnectionCount);
	}
	
	public int getAndDecreaseSendHalfRdbAndCloseConnectionCount() {
		return sendHalfRdbAndCloseConnectionCount.getAndDecrement();
	}
	
	public boolean isEof() {
		return eof;
	}
	
	public void setEof(boolean eof) {
		this.eof = eof;
	}

	public void increasePsyncCount() {
		psyncCount.incrementAndGet();
	}

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		server.initialize();
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		server.start();
	}

	@Override
	protected void doStop() throws Exception {
		server.stop();
		super.doStop();
	}

	@Override
	protected void doDispose() throws Exception {
		server.dispose();
		super.doDispose();
	}

	public boolean isPartialSyncFail() {
		return partialSyncFail;
	}

	public void setPartialSyncFail(boolean partialSyncFail) {
		this.partialSyncFail = partialSyncFail;
	}
}

