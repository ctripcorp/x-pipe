package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.command.AbstractCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class AggregatorStateSetterManager<K, S extends Collection<?>> {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private KeyedOneThreadTaskExecutor<K> keyedOneThreadTaskExecutor;

    private Function<K, S> getter;
    private BiConsumer<K, S> setter;

    public AggregatorStateSetterManager(Executor executors, Function<K, S> getter, BiConsumer<K, S> setter){
        this.getter = getter;
        this.setter = setter;
        keyedOneThreadTaskExecutor = new KeyedOneThreadTaskExecutor<K>(executors);
    }

    public void set(K k){
        logger.debug("[aggregator set]{}", k);
        keyedOneThreadTaskExecutor.execute(k, new AggregatorCheckAndSetTask(k));
    }

    public class AggregatorCheckAndSetTask extends AbstractCommand<Void> {

        private int retry = 3;
        private K k;

        public AggregatorCheckAndSetTask(K k){
            this.k = k;
        }

        @Override
        protected void doExecute() throws Exception {

            Exception exception = null;

            S newValue = null;

            if(getter != null){
                try{
                    newValue = getter.apply(k);
                }catch(Exception e){
                    logger.error("[doRun][aggregator]" + k, e);
                }
            }

            if (null == newValue) {
                logger.info("[doRun] unexpected null newValue");
                future().setFailure(new IllegalArgumentException("new value null"));
                return;
            }

            if(!newValue.isEmpty()) {

                for(int i=0; i < retry ;i++){
                    try{
                        logger.debug("[doRun][aggregator][begin]{}", k);
                        setter.accept(k, newValue);
                        logger.debug("[doRun][aggregator][end]{}", k);
                        exception = null;
                        break;
                    }catch (Exception e){
                        exception = e;
                        logger.error("[setter error][aggregator]" + k, e);
                    }
                }
            }else{
                logger.info("[doRun][aggregator][newValue empty, skip] {}", k);
            }

            if(exception != null){
                future().setFailure(exception);
            }else{
                future().setSuccess();
            }
        }

        @Override
        public String getName() {
            return "[AggregatorCheckAndSetTask]" + k;
        }

        @Override
        protected void doReset() {
        }
    }

}
