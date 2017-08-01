package com.ctrip.xpipe.concurrent;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.api.retry.RetryTemplate;
import com.ctrip.xpipe.retry.NoRetry;

/**
 * @author wenchao.meng
 *
 * Sep 7, 2016
 */
public class OneThreadTaskExecutor implements Destroyable{

	protected Logger logger = LoggerFactory.getLogger(getClass());

	private Executor executors;

	private Queue<Command<?>> tasks = new ConcurrentLinkedQueue<>();

	private AtomicBoolean isRunning = new AtomicBoolean(false);

	private AtomicBoolean destroyed = new AtomicBoolean(false);

	@SuppressWarnings("rawtypes")
	private RetryTemplate retryTemplate;

	public OneThreadTaskExecutor(Executor executors){
		this(new NoRetry<>(), executors);
	}

	public OneThreadTaskExecutor(RetryTemplate<?> retryTemplate, Executor executors){
		this.retryTemplate = retryTemplate;
		this.executors = executors;
	}

	public void executeCommand(Command<?> command){

		logger.debug("[executeCommand]{}", command);
        boolean offer = tasks.offer(command);
        if(!offer){
            throw new IllegalStateException("pool full:" + tasks.size());
        }
        executors.execute(new Task());
	}


	public class Task extends AbstractExceptionLogTask{

		@Override
		@SuppressWarnings({ "unchecked" })
		protected void doRun() throws Exception {

		    if(!isRunning.compareAndSet(false, true)){
                logger.debug("[doRun][already run]{}", this);
                return;
            }

		    try {

		        while(true){

                    Command<?> command = tasks.poll();
                    if(command == null){
                        break;
                    }

                    if(destroyed.get()){
                        break;
                    }

                    try {
                        executeCommand(command);
                    }catch (Exception e){
                        logger.error("[doRun]" + command, e);
                    }
                }
            }finally {
		        if(!isRunning.compareAndSet(true, false)){
		            logger.error("[doRun][already exit]");
                }
                if(tasks.size() > 0){
		            logger.info("[doRun][exit and new element come in again]");
		            doRun();
                }
            }
		}

        private void executeCommand(Command<?> command) throws Exception {
            logger.debug("[doRun]{}", command);
            retryTemplate.execute(command);

        }

    }

	@Override
	public void destroy() throws Exception {
	    logger.info("[destroy]{}", this);
	    destroyed.set(true);
	}
}
