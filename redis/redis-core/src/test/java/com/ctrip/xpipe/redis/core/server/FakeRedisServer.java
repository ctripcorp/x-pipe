package com.ctrip.xpipe.redis.core.server;


import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;

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
	private int sleepBeforeSendRdb = 0;
	private List<FakeRedisServerAction> commandListeners = new LinkedList<>();
	
	public FakeRedisServer(int port){
		this(port, 0);
	}
	public FakeRedisServer(int port, int sleepBeforeSendRdb){
		
		this.port = port;
		this.sleepBeforeSendRdb = sleepBeforeSendRdb;
		this.server = new Server(port, new IoActionFactory() {
			
			@Override
			public IoAction createIoAction() {
				return new FakeRedisServerAction(FakeRedisServer.this);
			}
		});
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

		rdbContent = AbstractTest.randomString(rdbSize);
		String prefix = String.format("rdboffset:%d--", rdbOffset);
		commands = prefix + AbstractTest.randomString(commandsLength - prefix.length());
		
		addCommands(commands);
	}
	
	private void addCommands(String commands) {
		for(FakeRedisServerAction listener :  commandListeners){
			listener.addCommands(commands);
		}
	}
	public int getCommandsLength() {
		return commandsLength;
	}
	
	public int getRdbSize() {
		return rdbSize;
	}

	public void addCommandsListener(FakeRedisServerAction fakeRedisServerAction) {
		
		logger.info("[addCommandsListener]{}", fakeRedisServerAction);
		fakeRedisServerAction.addCommands(commands);
		commandListeners.add(fakeRedisServerAction);
	}


	public Object currentCommands() {
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
		logger.info("[removeListener]{}", fakeRedisServerAction);
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

	public int getConnected() {
		return server.getConnected();
	}

}

