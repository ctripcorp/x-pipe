package com.ctrip.xpipe.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;


/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class DefaultListenableFuture<V> implements ListenableFuture<V>{

	private Logger logger = LoggerFactory.getLogger(getClass());

	private volatile Object result = null;
	
	private static final CauseHolder  CANCELLED_RESULT = new CauseHolder(new CancellationException());
	
	private static final String SUCCESS_NO_RESULT = "SUCCESS_NO_RESULT";
	
    private short waiters = 0;
    
    private final List<FutureListener<? super ListenableFuture<? super V>>> listeners = new LinkedList<>();

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		
		if(isDone()){
			return false;
		}
		
		synchronized(this){
			if(isDone()){
				return false;
			}
			result = CANCELLED_RESULT;
            if (hasWaiters()) {
                notifyAll();
            }
            notifyListeners();
		}
		return true;
	}

	@Override
	public boolean isCancelled() {
		return isDone() && result == CANCELLED_RESULT;
	}

	@Override
	public boolean isDone() {
		return result != null;
	}


	@Override
	public ListenableFuture<V> sync() throws InterruptedException, ExecutionException {
		get();
		return this;
	}

	
    @Override
    public V get() throws InterruptedException, ExecutionException {
    	
        await();

        Throwable cause = cause();
        if (cause == null) {
            return getNow();
        }
        if (cause instanceof CancellationException) {
            throw (CancellationException) cause;
        }
        throw new ExecutionException(cause);
    }

	@SuppressWarnings("unchecked")
	@Override
	public V getNow() {
		
		if(result instanceof CauseHolder || result == SUCCESS_NO_RESULT){
			return null;
		}
		return (V)result;
	}

	@Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (await(timeout, unit)) {
            Throwable cause = cause();
            if (cause == null) {
                return getNow();
            }
            if (cause instanceof CancellationException) {
                throw (CancellationException) cause;
            }
            throw new ExecutionException(cause);
        }
        throw new TimeoutException();
    }


    private static final class CauseHolder {
        final Throwable cause;
        CauseHolder(Throwable cause) {
            this.cause = cause;
        }
    }


	@Override
	public boolean isSuccess() {
		
		return result != null &&(result == SUCCESS_NO_RESULT || !(result instanceof CauseHolder));
	}

	@Override
	public Throwable cause() {
		
		if(result instanceof CauseHolder){
			return ((CauseHolder)result).cause;
		}
		return null;
	}

	@Override
	public void setSuccess() {
		setSuccess(null);
	}

	@Override
	public void setSuccess(V result) {
		
		if(isDone()){
			throw new IllegalStateException(alreadyComplete(result));
		}
		
		synchronized (this) {
			if(isDone()){
				throw new IllegalStateException(alreadyComplete(result));
			}
			
			if(result != null){
				this.result = result;
			}else{
				this.result = SUCCESS_NO_RESULT;
			}
            if (hasWaiters()) {
                notifyAll();
            }
            notifyListeners();
		}
	}

	private String alreadyComplete(Object given) {
		
		return "already completed!" + this.result + "->" + given;
	}

	@Override
	public void setFailure(Throwable cause) {
		
		if(isDone()){
			throw new IllegalStateException(alreadyComplete(cause));
		}
		
		synchronized (this) {
			if(isDone()){
				throw new IllegalStateException(alreadyComplete(cause));
			}
			this.result = new CauseHolder(cause);
            if (hasWaiters()) {
                notifyAll();
            }
            notifyListeners();
		}
	}

	@Override
	public Future<V> await() throws InterruptedException {
	        if (isDone()) {
	            return this;
	        }

	        if (Thread.interrupted()) {
	            throw new InterruptedException(toString());
	        }

	        synchronized (this) {
	            while (!isDone()) {
	                incWaiters();
	                try {
	                    wait();
	                } finally {
	                    decWaiters();
	                }
	            }
	        }
	        return this;
	}

	@Override
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return await0(unit.toNanos(timeout), true);
	}

    private boolean await0(long timeoutNanos, boolean interruptable) throws InterruptedException {
        if (isDone()) {
            return true;
        }

        if (timeoutNanos <= 0) {
            return isDone();
        }

        if (interruptable && Thread.interrupted()) {
            throw new InterruptedException(toString());
        }

        long startTime = System.nanoTime();
        long waitTime = timeoutNanos;
        boolean interrupted = false;

        try {
            synchronized (this) {
                if (isDone()) {
                    return true;
                }

                incWaiters();
                try {
                    for (;;) {
                        try {
                            wait(waitTime / 1000000, (int) (waitTime % 1000000));
                        } catch (InterruptedException e) {
                            if (interruptable) {
                                throw e;
                            } else {
                                interrupted = true;
                            }
                        }

                        if (isDone()) {
                            return true;
                        } else {
                            waitTime = timeoutNanos - (System.nanoTime() - startTime);
                            if (waitTime <= 0) {
                                return isDone();
                            }
                        }
                    }
                } finally {
                    decWaiters();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean hasWaiters() {
        return waiters > 0;
    }

    private void incWaiters() {
        if (waiters == Short.MAX_VALUE) {
            throw new IllegalStateException("too many waiters: " + this);
        }
        waiters ++;
    }

    private void decWaiters() {
        waiters --;
    }

	@Override
    public void addListener(FutureListener<? super ListenableFuture<? super V>> futureListener){

		if(isDone()){
			notifyListener(futureListener);
			return;
		}
		
		synchronized (this) {
			if(!isDone()){
				listeners.add(futureListener);
				return;
			}
		}
		
		notifyListener(futureListener);
	}

	private void notifyListener(FutureListener<? super ListenableFuture<? super V>> futureListener) {

		try{
			futureListener.operationComplete(this);
		}catch(Throwable th){
			logger.error("[notifyListener]" + this, th);
		}
	}
	
	private void notifyListeners() {
		
		for(FutureListener<? super ListenableFuture<? super V>> listener : listeners){
			notifyListener(listener);
		}
	}

}
