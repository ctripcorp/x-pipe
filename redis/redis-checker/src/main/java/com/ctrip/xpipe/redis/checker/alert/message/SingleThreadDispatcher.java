package com.ctrip.xpipe.redis.checker.alert.message;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.event.Dispatcher;
import com.ctrip.xpipe.redis.checker.alert.event.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public class SingleThreadDispatcher implements Dispatcher<AlertEntity> {

    private static final Logger logger = LoggerFactory.getLogger(SingleThreadDispatcher.class);

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
