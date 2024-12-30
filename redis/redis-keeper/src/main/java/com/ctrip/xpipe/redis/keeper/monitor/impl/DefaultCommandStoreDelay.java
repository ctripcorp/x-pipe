package com.ctrip.xpipe.redis.keeper.monitor.impl;


import com.ctrip.xpipe.exception.ExceptionLogWrapper;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

/**
 * @author wenchao.meng
 *
 * Nov 24, 2016
 */
public class DefaultCommandStoreDelay implements CommandStoreDelay{
	
	private static Logger logger = LoggerFactory.getLogger(DefaultCommandStoreDelay.class);
	
	public static int DEFAULT_DELAY_LOG_LIMIT_MICRO = Integer.parseInt(System.getProperty("DEFAULT_DELAY_LOG_LIMIT_MICRO", "5000"));
	
	private long lastActionTime = System.currentTimeMillis();
	
	public static int SUPPORT_SLAVES = Integer.parseInt(System.getProperty("SUPPORT_SLAVES", "10"));
	
	private int SUPPORT_OFFSETS = Integer.parseInt(System.getProperty("SUPPORT_OFFSETS", "30")); 
	
	private IntSupplier delayLogLimitMicro;

	private OffsetDelay[] offsetDelays = new OffsetDelay[SUPPORT_OFFSETS];
	
	private ExceptionLogWrapper exceptionLogWrapper = new ExceptionLogWrapper();
	
	private CommandStore commandStore;
	
	private int currentOffsetDelaysIndex = 0;
	

	public DefaultCommandStoreDelay(CommandStore commandStore){
		this(commandStore, () -> DEFAULT_DELAY_LOG_LIMIT_MICRO);
	}

	
	public DefaultCommandStoreDelay(CommandStore commandStore, IntSupplier delayLogLimitMicro){
		
		this.commandStore = commandStore;
		this.delayLogLimitMicro = delayLogLimitMicro;
		
		for(int i=0;i<offsetDelays.length;i++){
			offsetDelays[i] = new OffsetDelay();
		}
	}
	
	@Override
	public void beginWrite(){
		
		exceptionLogWrapper.execute(new Runnable() {
			
			@Override
			public void run() {
				updateLastActionTime();
				offsetDelays[currentOffsetDelaysIndex].beginWrite();
			}
		});
		
	}
	
	@Override
	public void endWrite(final long offset){
		
		exceptionLogWrapper.execute(new Runnable() {

			@Override
			public void run() {
				
				updateLastActionTime();
				offsetDelays[currentOffsetDelaysIndex].endWrite(offset);
				logger.trace("[endWrite]{}", offset);
				
				currentOffsetDelaysIndex = (currentOffsetDelaysIndex+1)%SUPPORT_OFFSETS;
				offsetDelays[currentOffsetDelaysIndex].reset();
			}
		});
	}

	public void endRead(final CommandsListener commandsListener, final long offset) {
		exceptionLogWrapper.execute(new Runnable() {

			@Override
			public void run() {
				updateLastActionTime();

				OffsetDelay offsetDelay = getOffsetDelay(offset);
				if(offsetDelay != null){
					offsetDelay.endRead(commandsListener);
				}
			}

		});
	}

	@Override
	public void beginSend(final CommandsListener commandsListener, final long offset){
		
		exceptionLogWrapper.execute(new Runnable() {

			@Override
			public void run() {
				updateLastActionTime();
				
				OffsetDelay offsetDelay = getOffsetDelay(offset);
				if(offsetDelay != null){
					offsetDelay.beginSend(commandsListener, offset);
				}
			}
			
		});
	}

	@Override
	public void flushSucceed(final CommandsListener commandsListener, final long offset){
		
		exceptionLogWrapper.execute(new Runnable() {
			@Override
			public void run() {
				updateLastActionTime();
				
				OffsetDelay offsetDelay = getOffsetDelay(offset);
				if(offsetDelay != null){
					offsetDelay.flushSucceed(commandsListener, offset);
				}
			}
		});
	}

	private OffsetDelay getOffsetDelay(long offset) {
		
		for(int i= 0; i < offsetDelays.length ; i++){
			
			OffsetDelay current = offsetDelays[i];
			if(current.getOffset() == offset){
				return current;
			}
		}
		logger.debug("[getOffsetDelay][null]{}", offset);
		return null;
	}

	
	public long getLastActionTime() {
		return lastActionTime;
	}

	private void updateLastActionTime() {
		this.lastActionTime = System.currentTimeMillis();
	}

	public class OffsetDelay{
		
		private boolean begin = false;
		private volatile long offset;
		private AtomicLong beginWriteTime = new AtomicLong();
		private AtomicLong endWriteTime = new AtomicLong();
		private ListenerDelay []listenerDelays = new ListenerDelay[SUPPORT_SLAVES];
		private AtomicInteger currentListenDelayIndex = new AtomicInteger();
		
		public OffsetDelay(){
			
			for(int i=0;i<listenerDelays.length;i++){
				listenerDelays[i] = new ListenerDelay(); 
			}
		}
		
		public void reset(){
			try{
				if(!begin){
					return;
				}
				if(currentListenDelayIndex.get() == 0){
					logger.debug("[reset][has not sent yet]{}, {}", commandStore, offset);
					return;
				}
				for(int i= 0; i < currentListenDelayIndex.get(); i++){
					listenerDelays[i].reset();
				}
			}finally{
				currentListenDelayIndex.set(0);
				offset = 0;
			}
		}

		public void beginWrite(){
			begin = true;
			this.beginWriteTime.set(System.nanoTime());
		}
		
		public void endWrite(long offset){
			this.offset = offset;
			this.endWriteTime.set(System.nanoTime());
			logIfShould(beginWriteTime.get(), endWriteTime.get(), "[endWrite]");
		}

		public void endRead(CommandsListener commandsListener) {
			ListenerDelay listenerDelay = getOrAddListenerDelay(commandsListener);
			if(listenerDelay != null){
				listenerDelay.endRead();
			}
		}

		public void beginSend(CommandsListener commandsListener, long offset){
			
			ListenerDelay listenerDelay = getOrAddListenerDelay(commandsListener);
			if(listenerDelay != null){
				listenerDelay.beginSend();
			}
		}

		public void flushSucceed(CommandsListener commandsListener, long offset){
			
			ListenerDelay listenerDelay = getListenerDelay(commandsListener);
			if(listenerDelay != null){
				listenerDelay.flushSucceed();
			}
		}
		
		public long getOffset() {
			return offset;
		}

		private ListenerDelay getListenerDelay(CommandsListener commandsListener) {

			int i = 0;
			for(; i < currentListenDelayIndex.get() ; i++){
				if(listenerDelays[i].commandsListener == commandsListener){
					return listenerDelays[i];
				}
			}
			return null;
		}

		private ListenerDelay getOrAddListenerDelay(CommandsListener commandsListener) {
			
			int i = 0;
			int current = currentListenDelayIndex.get();
			for(; i < current ; i++){
				if(listenerDelays[i].commandsListener == commandsListener){
					return listenerDelays[i];
				}
			}
			
			while(true){
			
				if( current >= SUPPORT_SLAVES){
					logger.error("[getOrAddListenerDelay][size exceend max support slaves]{}", SUPPORT_SLAVES);
					return null;
				}
				
				if(currentListenDelayIndex.compareAndSet(current, current + 1)){
					
					listenerDelays[current].setCommandsListener(commandsListener);
					return listenerDelays[current];
				}
				
				current++;
			}
		}

		
		public long currentOffset() {
			return offset;
		}

		public void nextOffset(long offset) {
			this.offset = offset;
		}
		
		public class ListenerDelay{
			
			private CommandsListener commandsListener;
			
			private AtomicLong endReadTime = new AtomicLong();

			private AtomicLong beginSendTime = new AtomicLong();
			
			private AtomicLong endSendTime = new AtomicLong();

			public void endRead() {
				this.endReadTime.set(System.nanoTime());
				logIfShould(endWriteTime.get(), endReadTime.get(), "[readOut]");
			}

			public void beginSend(){
				
				this.beginSendTime.set(System.nanoTime());
				logger.trace("[beginSend]{}", offset);
			}
			
			public void reset() {
				try{
					if(endSendTime.get() == 0 && System.nanoTime() - beginSendTime.get() >= delayLogLimitMicro.getAsInt() * 1000){
						logger.info("[reset][has not flushed]{}, off:{}, delay:{} micro", commandsListener, offset, (System.nanoTime() - beginSendTime.get())/1000);
					}
				}finally{
					beginSendTime.set(0);
					endSendTime.set(0);
					endReadTime.set(0);
					this.commandsListener = null;
				}
			}

			public void flushSucceed(){
				this.endSendTime.set(System.nanoTime());
				logIfShould(beginSendTime.get(), endSendTime.get(), "[flushSucceed]");
				
			}
			
			public CommandsListener getCommandsListener() {
				return commandsListener;
			}

			public void setCommandsListener(CommandsListener commandsListener) {
				this.commandsListener = commandsListener;
			}
		} 
	}

	protected boolean logIfShould(long begin, long end, String message) {
		
		long delayMicro = (end - begin)/1000;
		if(delayMicro >= delayLogLimitMicro.getAsInt()) {
			logger.info("{}, {}, {}, delay:{} micro", message, begin, end, delayMicro);
			return true;
		}
		return false;
	}

}
