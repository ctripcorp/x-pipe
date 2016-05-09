package com.ctrip.xpipe.redis.keeper.handler;

import java.util.Map;
import java.util.Set;

import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.protocal.protocal.BulkStringParser;

import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 下午3:51:33
 */
public class InfoHandler extends AbstractCommandHandler{

	@Override
	public String[] getCommands() {
		return new String[]{"info"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {

		
		boolean isDefault = false;
		boolean isAll = false;
		String section = null;
		
		if(args.length == 0){
			
			isDefault = true;
		}
		
		if(args.length == 1){
			
			if(args[0].equalsIgnoreCase("all")){
				isAll = true;
			}else{
				section = args[0];
			}
		}

		StringBuilder sb = new StringBuilder();
		RedisKeeperServer redisKeeperServer = redisClient.getRedisKeeperServer();

		server(isDefault, isAll, section, sb, redisKeeperServer);
		replication(isDefault, isAll, section, sb, redisKeeperServer);
		
		redisClient.sendMessage(new BulkStringParser(sb.toString()).format());
	}

	private void server(boolean isDefault, boolean isAll, String section, StringBuilder sb,
			RedisKeeperServer redisKeeperServer) {
		
		if(isDefault || isAll || "server".equalsIgnoreCase(section)){
			
			sb.append("# Server" + RedisProtocol.CRLF);
			sb.append(redisKeeperServer.info() + RedisProtocol.CRLF);
			
		}
		
	}

	private void replication(boolean isDefault, boolean isAll, String section, StringBuilder sb, RedisKeeperServer redisKeeperServer) {
		
		if(isDefault || isAll || "replication".equalsIgnoreCase(section)){
			
			sb.append("# Replication" + RedisProtocol.CRLF);
			sb.append("role:" + redisKeeperServer.role() + RedisProtocol.CRLF);
			Map<Channel, RedisClient> slaves = redisKeeperServer.slaves();
			sb.append("connected_slaves:" + slaves.size() + RedisProtocol.CRLF);
			int slaveIndex = 0;
			for(RedisClient client : slaves.values()){
				sb.append(String.format("slave%d:%s" + RedisProtocol.CRLF, slaveIndex, client.info()));
				slaveIndex++;
			}
			
			long beginOffset = redisKeeperServer.getBeginReploffset(), endOffset = redisKeeperServer.getEndReploffset();
			sb.append("master_repl_offset:" + redisKeeperServer.getEndReploffset() + RedisProtocol.CRLF);
			sb.append("repl_backlog_active:1" + RedisProtocol.CRLF);
			sb.append("repl_backlog_size:" + (endOffset - beginOffset + 1) + RedisProtocol.CRLF);
			sb.append("repl_backlog_first_byte_offset:" + beginOffset+ RedisProtocol.CRLF);
			sb.append("repl_backlog_histlen:" + (endOffset - beginOffset + 1)+ RedisProtocol.CRLF);
		}
	}

}
