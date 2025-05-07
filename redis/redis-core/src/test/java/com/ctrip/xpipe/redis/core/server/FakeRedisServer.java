package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
	private int commandsLength = 100;
	private int sendBatchSize = 200;
	private int sendBatchIntervalMilli = 10;
	
	private int port;
	private byte[] rdbContent = new byte[0];
	private String commands = "";
	private int    rdbOffset = 1;
	private Server server; 
	private String runId = RunidGenerator.DEFAULT.generateRunid();

	private boolean supportRordb = false;
	
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
		this(port, 0, "psync");
	}

	public FakeRedisServer(int port, String proto){
		this(port, 0, proto);
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

	public FakeRedisServer(int port, int sleepBeforeSendRdb, String proto){

		this.port = port;
		this.sleepBeforeSendRdb = sleepBeforeSendRdb;
		this.server = new Server(port, new IoActionFactory() {

			@Override
			public IoAction createIoAction(Socket socket) {
				return new FakeRedisServerAction(FakeRedisServer.this, socket, proto);
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

	public byte[] getRdbContent() {
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

	public synchronized void reGenerateRdb(boolean capaRordb) throws IOException {
		rdbOffset += commands.length();
		
		String prefix = String.format("rdb_rdboffset:%d--", rdbOffset);
		// "REDIS0009" + AUX(redis-ver:6.2.6) + SELECTDB 0
		byte[] magic = new byte[] {0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x39};
		byte[] aux = new byte[] {(byte)0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d,
				0x76, 0x65, 0x72, 0x05, 0x36, 0x2e, 0x32, 0x2e, 0x36};
		byte[] selectDb0 = new byte[] {(byte)0xfe, 0x00};

		ByteArrayOutputStream os = new ByteArrayOutputStream();
		os.write(magic);
		os.write(aux);
		// AUX(rordb:00001)
		if (capaRordb && this.supportRordb) os.write(new byte[] {(byte)0xfa, 0x05, 0x72, 0x6f, 0x72, 0x64, 0x62, 0x05, 0x30, 0x30, 0x30, 0x30, 0x31});
		os.write(selectDb0);
		os.write(prefix.getBytes());
		os.write(AbstractTest.randomString(rdbSize - prefix.length()).getBytes());
		rdbContent = os.toByteArray();

		if (commandsLength < 0) return;
		prefix = String.format("cmd_rdboffset:%d--", rdbOffset);
		commands = prefix + AbstractTest.randomString(commandsLength - prefix.length());
		addCommands(commands);
	}

	public synchronized void reGenerateRdb() throws IOException {
		reGenerateRdb(false);
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

	public boolean isSupportRordb() {
		return supportRordb;
	}

	public void setSupportRordb(boolean supportRordb) {
		this.supportRordb = supportRordb;
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

	@VisibleForTesting
    public void setRdbSize(int rdbSize) {
		this.rdbSize = rdbSize;
    }
}

