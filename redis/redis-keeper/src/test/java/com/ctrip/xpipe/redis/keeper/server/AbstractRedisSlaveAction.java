package com.ctrip.xpipe.redis.keeper.server;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import com.ctrip.xpipe.simpleserver.AbstractIoAction;
import com.ctrip.xpipe.simpleserver.SocketAware;


/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午3:08:46
 */
public abstract class AbstractRedisSlaveAction extends AbstractIoAction implements SocketAware{

	private String line;
	private Socket socket; 
	
	private boolean slaveof = false;
	private List<String> slaveOfCommands = new ArrayList<String>();
	
	@Override
	protected Object doRead(InputStream ins) throws IOException {
		
		line = readLine(ins);
		if(line != null){
			line = line.trim();
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
	public void setSocket(Socket socket) {
		this.socket = socket;
	}

	@Override
	protected void doWrite(OutputStream ous) throws IOException {
		
		if(line == null){
			logger.error("[doWrite]" + line);
			return;
		}
		
		byte []towrite = null;
		
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
			towrite = "+OK\r\n".getBytes();
		}
		
		if(towrite != null){
			ous.write(towrite);
			ous.flush();
		}
	}

	protected void slaveof(List<String> slaveOfCommands2) {
		
	}

	protected abstract String getInfo();

	protected String readLine(InputStream ins) throws IOException {
		
		StringBuilder sb = new StringBuilder();
		int last = 0;
		while(true){
			
			int data = ins.read();
			if(data == -1){
				return null;
			}
			sb.append((char)data);
			if(data == '\n' && last == '\r'){
				break;
			}
			last = data;
		}
		
		return sb.toString();
	}



}