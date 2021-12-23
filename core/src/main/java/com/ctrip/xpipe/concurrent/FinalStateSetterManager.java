package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * @author wenchao.meng
 *         <p>
 *         May 18, 2017
 */
public class FinalStateSetterManager<K, S> {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Map<K, S> map = new ConcurrentHashMap<K, S>();
    private Map<K, AtomicLong> lastCheckAndSetMap = new ConcurrentHashMap<K, AtomicLong>();

    @VisibleForTesting long LAZY_TIME_MILLI = 5 * 1000;

    private KeyedOneThreadTaskExecutor<K> keyedOneThreadTaskExecutor;

    private Function<K, S> getter;
    private BiConsumer<K, S> setter;

    public FinalStateSetterManager(Executor executors, Function<K, S> getter, BiConsumer<K, S> setter){
        this.getter = getter;
        this.setter = setter;
        keyedOneThreadTaskExecutor = new KeyedOneThreadTaskExecutor<K>(executors);
    }

    public boolean shouldCheckAndSet(S previous, S s, AtomicLong lastCheckAndSet) {
        if (!ObjectUtils.equals(previous, s)) {
            lastCheckAndSet.set(System.currentTimeMillis());
            return true;
        }
        long last = lastCheckAndSet.get();
        long now = System.currentTimeMillis();
        return ((now - last > LAZY_TIME_MILLI) && lastCheckAndSet.compareAndSet(last, now));
    }

    public void set(K k, S s){

        S previous = map.put(k, s);

        AtomicLong lastCheckAndSet = lastCheckAndSetMap.computeIfAbsent(k, key->new AtomicLong(0L));

        if(shouldCheckAndSet(previous, s, lastCheckAndSet)){

            logger.debug("[set]{},{}->{}", k, previous, s);
            keyedOneThreadTaskExecutor.execute(k, new CheckAndSetTask(k, s));
        }
    }

    public class CheckAndSetTask extends AbstractCommand<Void> {

        private int retry = 3;
        private K k;
        private S s;

        public CheckAndSetTask(K k, S s){
            this.k = k;
            this.s = s;
        }

        @Override
        protected void doExecute() throws Exception {

            Exception exception = null;

            S realValue = map.get(k);
            if(!ObjectUtils.equals(s, realValue)){
                logger.info("[doRun][current value not as expected, return]{},{},{}", k, s, realValue);
                future().setSuccess();
                return;
            }

            S currentValue = null;

            if(getter != null){
                try{
                    currentValue = getter.apply(k);
                }catch(Exception e){
                    logger.error("[doRun]" + k, e);
                }
            }

            if(!ObjectUtils.equals(s, currentValue)){

                for(int i=0; i < retry ;i++){
                    try{
                        logger.debug("[doRun][begin]{}, {}", k, s);
                        setter.accept(k, s);
                        logger.debug("[doRun][end]{}, {}", k, s);
                        break;
                    }catch (Exception e){
                        exception = e;
                        logger.error("[setter error]" + k +","+ s, e);
                    }
                }
            }else{
                logger.info("[doRun][already current state]{},{},{}", k, s, currentValue);
            }

            if(exception != null){
                future().setFailure(exception);
            }else{
                future().setSuccess();
            }
        }

        @Override
        public String getName() {
            return "[CheckAndSetTask]" + k;
        }

        @Override
        protected void doReset() {
        }
    }

}
