package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.payload.StringInOutPayload;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractPsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.ArrayUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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

		String info = String.format("+%s\r\n", AbstractPsync.PARTIAL_SYNC);
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

		String info = String.format("+%s %s %d\r\n", AbstractPsync.FULL_SYNC, fakeRedisServer.getRunId(), fakeRedisServer.getRdbOffset());
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
