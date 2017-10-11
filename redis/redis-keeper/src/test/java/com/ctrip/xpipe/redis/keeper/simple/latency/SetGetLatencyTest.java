package com.ctrip.xpipe.redis.keeper.simple.latency;


import com.ctrip.xpipe.api.monitor.DelayMonitor;
import com.ctrip.xpipe.monitor.DefaultDelayMonitor;
import redis.clients.jedis.Jedis;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public class SetGetLatencyTest extends AbstractLatencyTest{
	
	private DelayMonitor delayMonitor = new DefaultDelayMonitor("SET_GET", 5000);
	
	public static void main(String []argc) throws Exception{
		
		new SetGetLatencyTest(
				new InetSocketAddress("127.0.0.1", 6379),
//				new InetSocketAddress("127.0.0.1", 6379)
				new InetSocketAddress("127.0.0.1", 6479)
//				new InetSocketAddress("127.0.0.1", 8888)
				).start();
	}

	public SetGetLatencyTest(InetSocketAddress master, InetSocketAddress dest) {
		super(master, dest);
	}


	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		flushAll();
		executors.execute(new Send(master));
//		executors.execute(new Receive(dest));
	}
	
	private void flushAll() {
		
		try(Jedis jedis = new Jedis(master.getHostString(), master.getPort())){
			jedis.flushAll();
		}
	}

	abstract class AbstractMessage implements Runnable{

		protected Jedis jedis;
		
		public AbstractMessage(InetSocketAddress dest) {
			this.jedis = new Jedis(dest.getHostString(), dest.getPort());
		}

		@Override
		public void run() {
			try{
				doRun();
			}catch(Exception e){
				logger.error("[run]", e);
			}
		}
		
		protected abstract void doRun();
	}
	
	class Send extends AbstractMessage{
		
		public Send(InetSocketAddress dest) {
			super(dest);
		}

		public void doRun() {
			
			while(true){
				long current = increase();
				if(current < 0){
					break;
				}
				jedis.set(String.valueOf(current), String.valueOf(System.currentTimeMillis()));
			}
		}
	}
	
	class Receive extends AbstractMessage{

		public Receive(InetSocketAddress dest) {
			super(dest);
		}

		@Override
		protected void doRun() {
			
			for(long i = 1 ; i <= total;i++){
				
				while(true){
					
					String value = jedis.get(String.valueOf(i));
					if(value == null){
						try {
							TimeUnit.MICROSECONDS.sleep(1);
						} catch (InterruptedException e) {
						}
						continue;
					}
					
					delayMonitor.addData(Long.valueOf(value));
					break;
				}
			}
		}
	}
}
