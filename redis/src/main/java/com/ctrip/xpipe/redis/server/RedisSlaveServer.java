package com.ctrip.xpipe.redis.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.protocal.data.SimpleString;
import com.ctrip.xpipe.redis.rdb.RdbWriter;
import com.ctrip.xpipe.server.AbstractSlaveServer;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:08:26
 */
public class RedisSlaveServer extends AbstractSlaveServer {

	private Endpoint endpoint;
	private String masterRunId;
	private Long offset;
	
	private Charset charset = Codec.defaultCharset;

	private OutputStream commandOus;
	private RdbWriter rdbWriter;
	

	private InputStream socketIns;
	private OutputStream socketOus;
	private Socket socket = null;
	
	@SuppressWarnings("unused")
	private int retry = 3;

	public RedisSlaveServer(Endpoint endpoint, RdbWriter rdbWriter, OutputStream commandOus) {
		this(endpoint, rdbWriter, commandOus, "?", -1L, 3);
	}
	
	public RedisSlaveServer(Endpoint endpoint, RdbWriter rdbWriter, OutputStream commandOus, String masterRunId, Long offset, int retry) {

		this.endpoint = endpoint;
		this.rdbWriter = rdbWriter;
		this.commandOus = commandOus;
		this.masterRunId = masterRunId;
		this.offset = offset;
	}

	private void psync() throws IOException, XpipeException {
		
		Psync psync = new Psync(socketOus, socketIns, masterRunId, offset, rdbWriter, commandOus);
		psync.request();
	}

	private void connect() throws UnknownHostException, IOException {
		
		socket = new Socket(endpoint.getHost(), endpoint.getPort());
		socketOus = socket.getOutputStream();
		socketIns = socket.getInputStream();
		
	}


	private void sendListeningPort() throws IOException{
		
		byte [] request = getRequest("REPLCONF", "listening-port", "6379");
		socketOus.write(request);
		String response = new SimpleString().parse(socketIns);
		if(logger.isInfoEnabled()){
			logger.info("[sendListeningPort]" + response);
		}
		
	}
	
	
	private byte[] getRequest(String ...args) {
		
		StringBuilder sb = new StringBuilder();
		for(String arg : args){
			sb.append(arg);
			sb.append(' ');
		}
		sb.append("\r\n");
		return sb.toString().getBytes(charset);
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

}
