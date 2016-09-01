package com.ctrip.xpipe.redis.core;




import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.junit.Assert;
import org.junit.Before;
import org.xml.sax.SAXException;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.foundation.FakeFoundationService;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.FileUtils;
import com.ctrip.xpipe.utils.StringUtil;

import io.netty.buffer.ByteBufAllocator;
import redis.clients.jedis.Jedis;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午5:54:09
 */
public abstract class AbstractRedisTest extends AbstractTest{

	protected ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

	protected static final int runidLength = 40;
	
	private XpipeMeta xpipeMeta;

	@Before
	public void beforeAbstractRedisTest() throws SAXException, IOException{
		xpipeMeta = loadXpipeMeta(getXpipeMetaConfigFile());
	}
	

	protected String getXpipeMetaConfigFile(){
		return null;
	}


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


	protected Jedis createJedis(RedisMeta redisMeta) {
		
		String ip = StringUtil.isEmpty(redisMeta.getIp())? "localhost" : redisMeta.getIp();
		Jedis jedis = new Jedis(ip, redisMeta.getPort()); 
		logger.info("[createJedis]{}", jedis);
		return jedis;
	}

	protected void assertRedisEquals(RedisMeta redisMaster, RedisMeta[] slaves) {
		
		Map<String, String> values = new HashMap<>(); 
		Jedis jedis = createJedis(redisMaster);
		Set<String> keys = jedis.keys("*");
		for(String key : keys){
			values.put(key, jedis.get(key));
		}

		for(RedisMeta redisSlave : slaves){
			
			logger.info(remarkableMessage("[assertRedisEquals]redisSlave:" + redisSlave));
			
			Jedis slave = createJedis(redisSlave);
			int sizeExpected = values.size();
			int sizeReal = slave.keys("*").size();
			
			Assert.assertEquals(sizeExpected, sizeReal);
			
			for(Entry<String, String> entry : values.entrySet()){
				
				String realValue = slave.get(entry.getKey());
				Assert.assertEquals(entry.getValue(), realValue);
			}
		}
		
		
	}

	protected void sendRandomMessage(RedisMeta redisMeta, int count) {

		sendRandomMessage(redisMeta, count, 1024);
	}

	protected void sendRandomMessage(RedisMeta redisMeta, int count, int messageLength) {
		
		Jedis jedis = createJedis(redisMeta);
		logger.info("[sendRandomMessage][begin]{}", jedis);
		for(int i=0; i < count; i++){
			jedis.set(String.valueOf(i), randomString(messageLength));
			jedis.incr("incr");
		}
		logger.info("[sendRandomMessage][end  ]{}", jedis);
	}


	protected void executeCommands(String... args) throws ExecuteException, IOException {

		DefaultExecutor executor = new DefaultExecutor();
		executor.execute(CommandLine.parse(StringUtil.join(" ", args)));
	}

	protected void executeScript(String file, String... args) throws ExecuteException, IOException {

		URL url = getClass().getClassLoader().getResource(file);
		if (url == null) {
			url = getClass().getClassLoader().getResource("scripts/" + file);
			if (url == null) {
				throw new FileNotFoundException(file);
			}
		}
		DefaultExecutor executor = new DefaultExecutor();
		String command = "sh -v " + url.getFile() + " " + StringUtil.join(" ", args);
		executor.execute(CommandLine.parse(command));
	}

	protected SERVER_ROLE getRedisServerRole(RedisMeta slave) throws Exception {
		
		SimpleObjectPool<NettyClient>  clientPool = NettyPoolUtil.createNettyPool(new InetSocketAddress(slave.getIp(),slave.getPort()));
		String info = new InfoCommand(clientPool, "replication").execute().get();
		for(String line : info.split("\r\n")){
			String []parts = line.split(":");
			if(parts.length >= 2 && parts[0].equals("role")){
				String role = parts[1].trim();
				return SERVER_ROLE.of(role);
			}
		}
		return SERVER_ROLE.UNKNOWN;
	}

	protected XpipeMeta loadXpipeMeta(String configFile) throws SAXException, IOException {
		if(configFile == null){
			return null;
		}
		InputStream ins = FileUtils.getFileInputStream(configFile, getClass());
		return DefaultSaxParser.parse(ins);
	}
	
	protected List<RedisMeta> getRedises(String dc) {

		List<RedisMeta> result = new LinkedList<>();
		for(DcMeta dcMeta : getXpipeMeta().getDcs().values()){
			if(!dcMeta.getId().equals(dc)){
				continue;
			}
			for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
				for(ShardMeta shardMeta : clusterMeta.getShards().values()){
					result.addAll(shardMeta.getRedises());
				}
			}
		}
		return result;
	}

	protected List<RedisMeta> getRedisSlaves(String dc){

		List<RedisMeta> result = new LinkedList<>();
		
		for(RedisMeta redisMeta : getRedises(dc)){
			if(!redisMeta.isMaster()){
				result.add(redisMeta);
			}
		}
		return result;
	}

	protected DcMeta getDcMeta(String dc){
		return getXpipeMeta().getDcs().get(dc);
	}
	
	protected List<KeeperMeta> getDcKeepers(String dc, String clusterId, String shardId){
		return getXpipeMeta().getDcs().get(dc).getClusters().get(clusterId).getShards().get(shardId).getKeepers();
		
	}

	protected List<RedisMeta> getDcRedises(String dc, String clusterId, String shardId){
		return getXpipeMeta().getDcs().get(dc).getClusters().get(clusterId).getShards().get(shardId).getRedises();
		
	}

	protected List<DcMeta> getDcMetas(){
		
		List<DcMeta> result = new LinkedList<>();
		
		for(DcMeta dcMeta : getXpipeMeta().getDcs().values()){
			result.add(dcMeta);
		}
		return result;
	}

	protected RedisMeta getRedisMaster(){

		for(DcMeta dcMeta : getXpipeMeta().getDcs().values()){
			List<RedisMeta> redises = getRedises(dcMeta.getId());
			for(RedisMeta redisMeta : redises){
				if(redisMeta.isMaster()){
					return redisMeta;
				}
			}
		}
		return null;
	}
	
	
	protected DcMeta activeDc(){
		
		DcMeta activeDcMeta = null;
		
		for(DcMeta dcMeta : getXpipeMeta().getDcs().values()){
			if(activeDcMeta != null){
				break;
			}
			for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
				if(activeDcMeta != null){
					break;
				}
				for(ShardMeta shardMeta : clusterMeta.getShards().values()){
					if(activeDcMeta != null){
						break;
					}
					for(RedisMeta redisMeta : shardMeta.getRedises()){
						if(redisMeta.isMaster()){
							activeDcMeta = dcMeta;
							break;
						}
					}
				}
			}
		}
		return activeDcMeta; 
	}
	
	protected List<DcMeta> backupDcs(){
		
		List<DcMeta> result = new LinkedList<>();
		
		DcMeta active = activeDc();
		for(DcMeta dcMeta : getXpipeMeta().getDcs().values()){
			if(!dcMeta.getId().equals(active.getId())){
				result.add(dcMeta);
			}
		}
		return result;
	}

	public XpipeMeta getXpipeMeta() {
		return xpipeMeta;
	}

	protected FakeRedisServer startFakeRedisServer() throws Exception {
		return startFakeRedisServer(0);
	}

	protected FakeRedisServer startFakeRedisServer(int sleepBeforeSendRdb) throws Exception {
		int port = randomPort(6379, 6479); 
		FakeRedisServer fakeRedisServer = new FakeRedisServer(port, sleepBeforeSendRdb);
		fakeRedisServer.initialize();
		fakeRedisServer.start();
		add(fakeRedisServer);
		return fakeRedisServer;
	}

	
	protected String getAndSetDc(int index) {
		
		String dc = getDcMetas().get(index).getId(); 
		FakeFoundationService.setDataCenter(dc);
		return dc;
	}
}
