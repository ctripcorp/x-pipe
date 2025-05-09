package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.redis.core.protocal.Psync.FULL_SYNC;
import static com.ctrip.xpipe.redis.core.protocal.Psync.PARTIAL_SYNC;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public class FakeRedisServerAction extends AbstractRedisAction{
		
	private BlockingQueue<String> writeCommands = new LinkedBlockingQueue<>();
	private boolean waitAckToSendCommands = false;
	
	private FakeRedisServer fakeRedisServer;
	public FakeRedisServerAction(FakeRedisServer fakeRedisServer, Socket socket) {
		super(socket);
		this.fakeRedisServer = fakeRedisServer;
	}

	public FakeRedisServerAction(FakeRedisServer fakeRedisServer, Socket socket, String proto) {
		super(socket);
		this.fakeRedisServer = fakeRedisServer;
		this.proto = proto;
	}

	@Override
	protected String getInfo() {
		return String.format("port:%d", fakeRedisServer.getPort());
	}

	@Override
	protected void handlePsync(OutputStream ous, String line) throws IOException, InterruptedException {
		
		logger.info("[handlePsync]{}", line);
		fakeRedisServer.increasePsyncCount();
		String []sp = line.split("\\s+");
		if(sp[1].equalsIgnoreCase("?")){
			handleFullSync(ous);
			return;
		}
		
		long offset = Long.parseLong(sp[2]);
		handlePartialSync(ous, offset);
	}

	@Override
	protected void handleXsync(OutputStream ous, String line) throws IOException, InterruptedException {

		logger.info("[handleXsync]{}", line);
		fakeRedisServer.increasePsyncCount();
		String []sp = line.split("\\s+");
		if(sp[1].equalsIgnoreCase("?")){
			handleXsynFull(ous);
			return;
		}
		handleXsynContinue(ous);
	}

	protected void handleXsynContinue(OutputStream ous) throws IOException, InterruptedException {
		logger.info("[handleContinueXSync]{}", getSocket());
		fakeRedisServer.addCommandsListener(this);
		String info = "+XCONTINUE GTID.SET 7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-21,1777955e932bed5eb321a58fbc2132cba48f026f:1-2 MASTER.UUID f804ec9202b78f93ec8fe10df9b5b60b627dda2f REPLID eafdcd2f70b9344ca97d997663566a48e0e913ad REPLOFF 589\r\n";
		ous.write(info.getBytes());
		ous.flush();

		if(!waitAckToSendCommands){
			writeCommands(ous);
		}
	}

	protected void handleXsynFull(OutputStream ous) throws IOException, InterruptedException {

		logger.info("[handleFullXSync]{}", getSocket());
		//fakeRedisServer.reGenerateRdb(this.capaRordb);
		fakeRedisServer.addCommandsListener(this);

		try {
			Thread.sleep(fakeRedisServer.getSleepBeforeSendFullSyncInfo());
		} catch (InterruptedException e) {
		}

		String info = "+XFULLRESYNC GTID.LOST \"\" MASTER.UUID 7ca392ffb0fa8415cbf6a88bb7937f323c7367ac REPLID d52148a01c3302e95874864a98b3e9f5bc421f3c REPLOFF 0\r\n";
		ous.write(info.getBytes());
		ous.flush();

		ScheduledFuture<?> future = null;
		if(fakeRedisServer.isSendLFBeforeSendRdb()){
			future = sendLfToSlave(ous);
		}

		try {
			Thread.sleep(fakeRedisServer.getSleepBeforeSendRdb());
		} catch (InterruptedException e) {
		}

		if(future != null){
			future.cancel(true);
		}

		ous.write("$316\r\n".getBytes());
		String rdbFilePath = "src/test/resources/GtidTest/dump.rdb";
		try (FileInputStream fileInputStream = new FileInputStream(rdbFilePath)) {
			byte[] buffer = new byte[8192]; // 缓冲区大小
			int bytesRead;
			while ((bytesRead = fileInputStream.read(buffer)) != -1) {
				// 将读取的字节写入 OutputStream
				ous.write(buffer, 0, bytesRead);
			}
		}

		ous.flush();

		if(!waitAckToSendCommands){
			writeCommands(ous);
		}
	}

	private void handlePartialSync(OutputStream ous, long offset) throws IOException, InterruptedException {

		if(fakeRedisServer.isPartialSyncFail()){
			logger.info("[handlePartialSync]partial sync fail, close socket");
			closeSocket();
			return;
		}
		
		logger.info("[handlePartialSync]");
		int index = (int) (offset - fakeRedisServer.getRdbOffset() -1);
		if(index<0){
			logger.info("[indx < 0][full sync]");
			handleFullSync(ous);
			return;
		}

		String info = String.format("+%s\r\n", PARTIAL_SYNC);
		ous.write(info.getBytes());

		fakeRedisServer.addCommandsListener(this);

		writeCommands(ous);
		
	}


	private void writeCommands(OutputStream ous) throws IOException, InterruptedException {
		
		startReadThread();
		
		while(true){
			
			String command = writeCommands.poll(10, TimeUnit.MILLISECONDS);
			if(getSocket().isClosed()){
				logger.info("[writeCommands][closed]");
				return;
			}
			if(command == null){
				continue;
			}
			String []sps = split(command);
			for(String sp : sps){
				logger.debug("[writeCommands]{}, {}", socket, sp.length());
				ous.write(sp.getBytes());
				ous.flush();
				TimeUnit.MILLISECONDS.sleep(fakeRedisServer.getSendBatchIntervalMilli());
			}
		}
	}

	private void startReadThread() {
		
		new Thread(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				InputStream ins = getSocket().getInputStream();
				while(true){
					int data = ins.read();
					if(data == -1){
						logger.info("[doRun]read -1, close socket:{}", getSocket());
						getSocket().close();
						return;
					}
				}
			}
		}).start();
	}

	private String[] split(String command) {
		
		int splitLen = fakeRedisServer.getSendBatchSize();
		return StringUtil.splitByLen(command, splitLen);
	}

	private void handleFullSync(final OutputStream ous) throws IOException, InterruptedException {
		
		logger.info("[handleFullSync]{}", getSocket());
		fakeRedisServer.reGenerateRdb(this.capaRordb);
		fakeRedisServer.addCommandsListener(this);

		try {
			Thread.sleep(fakeRedisServer.getSleepBeforeSendFullSyncInfo());
		} catch (InterruptedException e) {
		}

		String info = String.format("+%s %s %d\r\n", FULL_SYNC, fakeRedisServer.getRunId(), fakeRedisServer.getRdbOffset());
		ous.write(info.getBytes());
		ous.flush();
		
		ScheduledFuture<?> future = null;
		if(fakeRedisServer.isSendLFBeforeSendRdb()){
			future = sendLfToSlave(ous);
		}
		
		try {
			Thread.sleep(fakeRedisServer.getSleepBeforeSendRdb());
		} catch (InterruptedException e) {
		}
		
		if(future != null){
			future.cancel(true);
		}
		
		byte []rdb = null;
		int  rdbStartPos = 0;
		byte[] rdbContent = fakeRedisServer.getRdbContent();
		if(fakeRedisServer.isEof()){
			String mark = RunidGenerator.DEFAULT.generateRunid();
			String header = "$EOF:" + mark + "\r\n" ;
			rdbStartPos = header.length();

			ByteArrayOutputStream os = new ByteArrayOutputStream();
			os.write(header.getBytes());
			os.write(rdbContent);
			os.write(mark.getBytes());

			rdb = os.toByteArray();
			waitAckToSendCommands = true;
		}else{
			InOutPayload payload = new ByteArrayOutputStreamPayload();
			payload.startInput();
			payload.in(Unpooled.wrappedBuffer(rdbContent));
			payload.endInput();

			RdbBulkStringParser bulkStringParser = new RdbBulkStringParser(payload);
			ByteBuf byteBuf = bulkStringParser.format();
			rdb = ByteBufUtils.readToBytes(byteBuf);
			rdbStartPos = 3 + String.valueOf(rdbContent.length).length();
			waitAckToSendCommands = false;
		}
		if(logger.isDebugEnabled()){
			logger.debug("[handleFullSync]{}, {}", getSocket(), new String(rdb));
		}
		
		if(fakeRedisServer.getAndDecreaseSendHalfRdbAndCloseConnectionCount() > 0){
			ous.write(rdb, 0, rdbStartPos + rdbContent.length/2);
			ous.flush();
			logger.info("[handleFullSync]halfsync after write half data,close socket:{}", getSocket());
			ous.close();
		}else{
			ous.write(rdb);
			ous.flush();
		}
		
		if(!waitAckToSendCommands){
			writeCommands(ous);
		}
	}

	protected byte[] handleConfigGet(String line) throws NumberFormatException, IOException {
		String []sp = line.split("\\s+");
		String param = sp[2];
		if (param.equalsIgnoreCase("swap-repl-rordb-sync") && fakeRedisServer.isSupportRordb()) {
			return "*2\r\n$20\r\nswap-repl-rordb-sync\r\n$3\r\nyes\r\n".getBytes();
		}
		return super.handleConfigGet(line);
	}

	@Override
	protected boolean isKeeper() {
		return fakeRedisServer.isKeeper();
	}

	@Override
	protected void replconfAck(long ackPos) throws IOException, InterruptedException{
		super.replconfAck(ackPos);
		
		if(waitAckToSendCommands){
			waitAckToSendCommands = false;
			logger.info("[replconfAck][ack][sendCommands]{}", ackPos);
			writeCommands(getSocket().getOutputStream());
		}
	}

	private ScheduledFuture<?> sendLfToSlave(final OutputStream ous) {

		return scheduled.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				
				try {
					ous.write("\n".getBytes());
				} catch (IOException e) {
					logger.error("[run]" + this, e);
				}
			}
		}, 0, 100, TimeUnit.MILLISECONDS);
	}

	public void addCommands(String commands){
		writeCommands.offer(commands);
	}
	
	@Override
	public void setDead() {
		logger.info("[setDead]{}", getSocket());
		fakeRedisServer.removeListener(this);
	}
	
	
	public static void main(String []argc){
		
//		System.out.println(StringUtil.join(",", new  FakeRedisServerAction(null).split("0123456789")));
		
		
	}
}
