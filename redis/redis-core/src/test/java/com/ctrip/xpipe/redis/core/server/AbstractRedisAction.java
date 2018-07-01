package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.simpleserver.AbstractIoAction;
import com.ctrip.xpipe.simpleserver.SocketAware;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public abstract class AbstractRedisAction extends AbstractIoAction implements SocketAware{

	private byte[] OK = "+OK\r\n".getBytes();
	private String line;
	
	private boolean slaveof = false;
	private List<String> slaveOfCommands = new ArrayList<String>();

	public AbstractRedisAction(Socket socket) {
		super(socket);
	}

	@Override
	protected Object doRead(InputStream ins) throws IOException {
		
		line = readLine(ins);
		if(line != null){
			line = line.trim().toLowerCase();
		}
		if(logger.isInfoEnabled()){
			logger.info("[doRead]" + getLogInfo() + ":" + line);
		}
		
		if(slaveof && !line.startsWith("$") && !line.startsWith("*")){
			slaveOfCommands.add(line);
		}
		return line;
	}
	
	protected String getLogInfo() {
		return socket.toString();
	}
	
	@Override
	protected void doWrite(OutputStream ous, Object readResult) throws IOException {
		
		if(line == null){
			logger.error("[doWrite]" + line);
			return;
		}
		
		byte []towrite = null;

		if(line.startsWith("replconf")){
			towrite = handleReplconf(line);
		}
		
		if(line.equalsIgnoreCase("PING")){
			towrite = "+PONG\r\n".getBytes();
		}
		
		if(line.equalsIgnoreCase("INFO")){
			String info = getInfo();
			String infoCommand = "$" + info.length() + "\r\n" + info + "\r\n";
			towrite = infoCommand.getBytes();
		}
		
		if(line.equalsIgnoreCase("PUBLISH")){
			towrite = ":1\r\n".getBytes();
		}
		
		if(line.equalsIgnoreCase("setname")){
			towrite = "+OK\r\n".getBytes();
		}
		
		if(line.equalsIgnoreCase("MULTI")){
			towrite = "+OK\r\n".getBytes();
		}
		
		if(line.equalsIgnoreCase("slaveof")){
			towrite = "+QUEUED\r\n".getBytes();
			slaveof = true;
			slaveOfCommands.clear();
			slaveOfCommands.add(line);
		}

		if(slaveof && line.startsWith("*")){
			slaveof = false;
			slaveof(slaveOfCommands);
		}
		
		if(line.equalsIgnoreCase("kill") || line.equalsIgnoreCase("rewrite") ){
			towrite = "+QUEUED\r\n".getBytes();
		}
		
		if(line.equalsIgnoreCase("exec")){
			towrite = OK;
		}
		
		boolean writeToWrite = true;
		
		if(line.startsWith("psync")){
			try {
				writeToWrite = false;
				handlePsync(ous, line);
			} catch (InterruptedException e) {
				logger.error("[handlepsync]", e);
			}
		}

		if(writeToWrite){
			if(towrite != null){
				ous.write(towrite);
				ous.flush();
			}else{
				ous.write("-unsupported command\r\n".getBytes());
				ous.flush();
			}
		}
	}

	protected byte[] handleReplconf(String line) throws NumberFormatException, IOException{
		
		String []sp = line.split("\\s+");
		if(sp[1].equals("ack")){
			try {
				replconfAck(Long.parseLong(sp[2]));
			} catch (InterruptedException e) {
				logger.error("[handleReplconf]", e);
			}
			return null;
		}
		return OK;
	}

	protected void replconfAck(long ackPos) throws IOException, InterruptedException {
		
	}

	protected void handlePsync(OutputStream ous, String line) throws IOException, InterruptedException {
		
	}

	protected void slaveof(List<String> slaveOfCommands2) {
		
	}

	protected abstract String getInfo();
}
