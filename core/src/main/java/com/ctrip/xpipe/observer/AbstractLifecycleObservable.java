package com.ctrip.xpipe.observer;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author wenchao.meng
 *
 * May 18, 2016 4:14:57 PM
 */
public abstract class AbstractLifecycleObservable extends AbstractLifecycle implements Observable, Lifecycle{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	private ReadWriteLock observersLock = new ReentrantReadWriteLock();
	private List<Observer> observers = new ArrayList<>();
	
	private Executor executors = MoreExecutors.directExecutor();

	
	public AbstractLifecycleObservable() {
	}

	public AbstractLifecycleObservable(Executor executors) {
		this.executors = executors;
	}
	
	@Override
	public void addObserver(Observer observer) {

		try{
			observersLock.writeLock().lock();
			observers.add(observer);
		}finally {
			observersLock.writeLock().unlock();
		}
		
	}

	public void removeObserver(Observer observer) {

		try {
			observersLock.writeLock().lock();
			observers.remove(observer);
		}finally {
			observersLock.writeLock().unlock();
		}
	}

	public void setExecutors(Executor executors) {
		this.executors = executors;
	}

	protected void notifyObservers(final Object arg){
		
		Object []tmpObservers;

		try {
			observersLock.readLock().lock();
			tmpObservers = observers.toArray();
		}finally {
			observersLock.readLock().unlock();
		}
		
		for(final Object observer : tmpObservers){

				beginNotifyObserver(observer);

				executors.execute(new Runnable() {
					@Override
					public void run() {
						try{
							((Observer)observer).update(arg, AbstractLifecycleObservable.this);
						}catch(Exception e){
							logger.error("[notifyObservers]" + observer, e);
						}
					}
				});
		}
	}

	protected void beginNotifyObserver(Object observer){
	}
}
