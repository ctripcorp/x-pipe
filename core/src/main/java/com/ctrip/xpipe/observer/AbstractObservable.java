package com.ctrip.xpipe.observer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author wenchao.meng
 *
 * May 18, 2016 4:14:57 PM
 */
public abstract class AbstractObservable implements Observable{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	private ReadWriteLock observersLock = new ReentrantReadWriteLock();
	private List<Observer> observers = new ArrayList<>();
		
	private Executor executors = MoreExecutors.directExecutor();

	public AbstractObservable() {
	}

	public AbstractObservable(Executor executors) {
		this.executors = executors;
	}

	public void setExecutors(Executor executors) {
		this.executors = executors;
	}

	@Override
	public void addObserver(Observer observer) {

		try {
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
	
	
	protected void notifyObservers(final Object arg){
		
		Object []tmpObservers;

		try {
			observersLock.readLock().lock();
			tmpObservers = observers.toArray();
		}finally {
			observersLock.readLock().unlock();
		}
		
		for(final Object observer : tmpObservers){
			
			executors.execute(new Runnable() {
				@Override
				public void run() {
					try{
						((Observer)observer).update(arg, AbstractObservable.this);
					}catch(Exception e){
						logger.error("[notifyObservers]" + observer, e);
					}
				}
			});
		}
	}
}
