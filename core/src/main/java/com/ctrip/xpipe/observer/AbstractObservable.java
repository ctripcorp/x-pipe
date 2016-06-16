package com.ctrip.xpipe.observer;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;

/**
 * @author wenchao.meng
 *
 * May 18, 2016 4:14:57 PM
 */
public abstract class AbstractObservable implements Observable{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private List<Observer> observers = new LinkedList<>();
	
	@Override
	public synchronized void addObserver(Observer observer) {
		
		observers.add(observer);
		
	}

	public synchronized void removeObserver(Observer observer) {
		
		observers.remove(observer);
	}
	
	
	protected void notifyObservers(Object arg){
		
		Object []tmpObservers;
		
		synchronized (observers) {
			tmpObservers = observers.toArray();
		}
		
		for(Object observer : tmpObservers){
			
			try{
				((Observer)observer).update(arg, this);
			}catch(Exception e){
				logger.error("[notifyObservers]" + observer, e);
			}
		}
	}
}
