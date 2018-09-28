package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class FixedObjectPool<T> implements SimpleObjectPool<T>{
	
	private AtomicReference<T> objRef = new AtomicReference<T>();
	private Semaphore semaphore;
	private int permits;

	public FixedObjectPool(T obj) {
		this(obj, 1);
	}

	public FixedObjectPool(T obj, int permits) {
		objRef.set(obj);
		semaphore = new Semaphore(permits);
	}
	

	@Override
	public T borrowObject() throws BorrowObjectException {

		if(semaphore.tryAcquire()){
			T obj = objRef.get();
			return obj;
		}else{
			throw new BorrowObjectException("no object left:" + permits);
		}
	}

	@Override
	public void returnObject(T obj) throws ReturnObjectException {
		semaphore.release();
	}
	
	public T getObject(){
		return objRef.get();
	}

	@Override
	public void clear() {
		semaphore = new Semaphore(permits);		
	}

	@Override
	public String desc() {
		return objRef.get().toString();
	}

}
