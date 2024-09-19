package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.command.AbstractCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class AggregatorStateSetterManager<K, S extends Set> {

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

            S currentValue = null;

            if(getter != null){
                try{
                    currentValue = getter.apply(k);
                }catch(Exception e){
                    logger.error("[doRun][aggregator]" + k, e);
                }
            }

            if(currentValue != null && !currentValue.isEmpty()){

                for(int i=0; i < retry ;i++){
                    try{
                        logger.debug("[doRun][aggregator][begin]{}", k);
                        setter.accept(k, currentValue);
                        logger.debug("[doRun][aggregator][end]{}", k);
                        break;
                    }catch (Exception e){
                        exception = e;
                        logger.error("[setter error][aggregator]" + k, e);
                    }
                }
            }else{
                logger.info("[doRun][aggregator][already current state]{},{}", k, currentValue);
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
