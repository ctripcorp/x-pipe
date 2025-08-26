package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.simpleserver.AbstractIoAction;
import com.ctrip.xpipe.simpleserver.SocketAware;
import com.ctrip.xpipe.utils.StringUtil;

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
	private byte[] ERR = "-ERR \r\n".getBytes();

	public String proto = "psynx";

	private String line;
	
	private boolean slaveof = false;
	private List<String> slaveOfCommands = new ArrayList<String>();

	protected boolean capaRordb = false;

	public AbstractRedisAction(Socket socket) {
		super(socket);
	}

	public void setProto(String proto) {
		this.proto = proto;
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
		if (line.startsWith("config get")) {
			towrite = handleConfigGet(line);
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
				if("psync".equals(proto)){
					handlePsync(ous, line);
				} else {
					handleXsync(ous, line);
				}
			} catch (InterruptedException e) {
				logger.error("[handlepsync]", e);
			}
		}

		if(line.startsWith("xsync")){
			writeToWrite = false;
			try {
				handleXsync(ous, line);
			} catch (InterruptedException e) {
				logger.error("[handlexsync]", e);
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

	protected byte[] handleConfigGet(String line) throws NumberFormatException, IOException {
		return "*0\r\n".getBytes();
	}

	protected byte[] handleReplconf(String line) throws NumberFormatException, IOException{

		logger.info("[handleReplconf] " + line);
		String []sp = line.split("\\s+");
		String option = sp[1];
		if(option.equals("ack")){
			try {
				replconfAck(Long.parseLong(sp[2]));
			} catch (InterruptedException e) {
				logger.error("[handleReplconf]", e);
			}
			return null;
		}

		if(option.equals("keeper")){
			if(!isKeeper()){
				return ERR;
			}else{
				return OK;
			}

		}

		if (option.equals("capa")) {
			for (int i = 2; i < sp.length; i++) {
				if (sp[i].equalsIgnoreCase("rordb")) {
					logger.info("[handleReplconf] set rordb true");
					this.capaRordb = true;
				}
			}
		}

		return OK;
	}

	protected boolean isKeeper(){
		return false;
	}

	protected void replconfAck(long ackPos) throws IOException, InterruptedException {
		
	}

	protected void handlePsync(OutputStream ous, String line) throws IOException, InterruptedException {
		
	}

	protected void handleXsync(OutputStream ous, String line) throws IOException, InterruptedException {

	}



	protected void slaveof(List<String> slaveOfCommands2) {
		
	}

	protected abstract String getInfo();
}
