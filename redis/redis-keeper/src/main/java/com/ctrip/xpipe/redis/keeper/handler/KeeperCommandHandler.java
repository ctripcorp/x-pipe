package com.ctrip.xpipe.redis.keeper.handler;


import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;

import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.WHITE_SPACE;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public class KeeperCommandHandler extends AbstractCommandHandler{

	@Override
	public String[] getCommands() {
		return new String[]{"keeper"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) throws Exception {
		
		if(args.length >= 1){
			
			if(args[0].equalsIgnoreCase(AbstractKeeperCommand.GET_STATE)){
				
				KeeperState keeperState = redisClient.getRedisKeeperServer().getRedisKeeperServerState().keeperState();
				redisClient.sendMessage(new SimpleStringParser(keeperState.toString()).format());
			}else if(args[0].equalsIgnoreCase(AbstractKeeperCommand.SET_STATE)){
				
				if(args.length >= 4){
					KeeperState keeperState = KeeperState.valueOf(args[1]);
					Endpoint masterAddress = getMasterAddress(args);
					doSetKeeperState(redisClient, keeperState, masterAddress);
				}else{
					throw new IllegalArgumentException("setstate argument error:" + StringUtil.join(" ", args));
				}
			}else{
				throw new IllegalStateException("unknown command:" + args[0]);
			}
		}
	}

	private void doSetKeeperState(RedisClient redisClient, KeeperState keeperState, Endpoint masterAddress) {

		RedisKeeperServer redisKeeperServer = redisClient.getRedisKeeperServer();

		RedisKeeperServerState currentState = redisKeeperServer.getRedisKeeperServerState();
		try{
			switch(keeperState){
				case ACTIVE:
					currentState.becomeActive(masterAddress);
					break;
				case BACKUP:
					currentState.becomeBackup(masterAddress);
					break;
				case UNKNOWN:
					throw new IllegalStateException("state can not change to unknown!");
				default:
					throw new IllegalStateException("unrecognised state:" + keeperState);
			}
			redisClient.sendMessage(new SimpleStringParser(RedisProtocol.OK).format());
		}catch(Exception e){
			logger.error("[doSetKeeperState]" + String.format("%s, %s, %s", redisClient, keeperState, masterAddress), e);
			redisClient.sendMessage(new RedisErrorParser(e.getMessage()).format());
		}
	}

	private Endpoint getMasterAddress(String[] args) {
		if(containsProxyProtocol(args)) {
			return new ProxyEnabledEndpoint(args[2], Integer.parseInt(args[3]), getProxyProtocol(args));
		} else {
			return new DefaultEndPoint(args[2], Integer.parseInt(args[3]));
		}
	}

	private boolean containsProxyProtocol(String[] args) {
		return args.length > 4 && "proxy".equalsIgnoreCase(args[4]);
	}


	// setstate ACTIVE 127.0.0.1 6379 PROXY ROUTE PROXYTCP://127.0.0.1:80,PROXYTCP://127.0.0.2;80 TCP
	@VisibleForTesting
	protected ProxyConnectProtocol getProxyProtocol(String[] args) {
		String[] protocolArr = new String[args.length - 4];
		System.arraycopy(args, 4, protocolArr, 0, protocolArr.length);
		String scheme = protocolArr[protocolArr.length - 1];
		protocolArr[protocolArr.length - 1] = String.format("%s://%s:%s", scheme, args[2], args[3]);
		String protocol = StringUtil.join(WHITE_SPACE, protocolArr);
		logger.info("[getProxyProtocol] protocol: {}", protocol);
		return new DefaultProxyConnectProtocolParser().read(protocol);
	}
}
