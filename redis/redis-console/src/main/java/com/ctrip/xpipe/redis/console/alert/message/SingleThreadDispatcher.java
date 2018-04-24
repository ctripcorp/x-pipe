package com.ctrip.xpipe.redis.console.alert.message;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.job.event.Dispatcher;
import com.ctrip.xpipe.redis.console.job.event.Subscriber;

import java.util.concurrent.Executor;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public class SingleThreadDispatcher implements Dispatcher<AlertEntity> {

    private KeyedOneThreadTaskExecutor<ALERT_TYPE> keyedOneThreadTaskExecutor;

    public SingleThreadDispatcher(Executor executor) {
        keyedOneThreadTaskExecutor = new KeyedOneThreadTaskExecutor<>(executor);
    }

    @Override
    public void dispatch(Subscriber<AlertEntity> subscriber, AlertEntity alertEntity) {
        keyedOneThreadTaskExecutor.execute(alertEntity.getAlertType(), new AbstractCommand<Void>() {
            @Override
            protected void doExecute() throws Exception {
                logger.debug("[dispatch]Subscriber: {}, alert: {}", subscriber, alertEntity);
                subscriber.processData(alertEntity);
                logger.debug("[dispatch]finished");
                future().setSuccess();
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return SingleThreadDispatcher.class.getSimpleName();
            }
        });
    }

}
