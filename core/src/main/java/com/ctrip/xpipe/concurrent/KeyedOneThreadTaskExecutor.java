package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * @author wenchao.meng
 *
 * Jan 3, 2017
 */
public class KeyedOneThreadTaskExecutor<K> implements Destroyable{
	
	private static Logger logger = LoggerFactory.getLogger(KeyedOneThreadTaskExecutor.class);
	
	private Map<K, OneThreadTaskExecutor> keyedExecutor = new ConcurrentHashMap<>();

	protected Executor executors;
	
	public KeyedOneThreadTaskExecutor(Executor executors){
		this.executors = executors;
	}

	public void execute(K key, Command<?> command){
		
		OneThreadTaskExecutor oneThreadTaskExecutor = getOrCreate(key);
		oneThreadTaskExecutor.executeCommand(command);
	}

	
	protected OneThreadTaskExecutor getOrCreate(K key) {
		
		return MapUtils.getOrCreate(keyedExecutor, key, new ObjectFactory<OneThreadTaskExecutor>() {
			
			@Override
			public OneThreadTaskExecutor create() {
				return createTaskExecutor();
			}
		});
	}

	protected OneThreadTaskExecutor createTaskExecutor() {
		return new OneThreadTaskExecutor(executors);
	}

	@Override
	public void destroy() throws Exception {
		
		logger.info("[destroy]{}", this);
		for(Entry<K, OneThreadTaskExecutor> entry : keyedExecutor.entrySet()){
			entry.getValue().destroy();
		}
	}
}
