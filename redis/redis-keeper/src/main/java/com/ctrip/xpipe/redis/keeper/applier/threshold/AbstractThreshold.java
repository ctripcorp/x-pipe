package com.ctrip.xpipe.redis.keeper.applier.threshold;

import com.ctrip.xpipe.utils.Gate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Slight
 * <p>
 * Dec 09, 2022 11:41
 */
public abstract class AbstractThreshold {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Gate gate = new Gate(getClass().getSimpleName());

    protected AtomicLong accumulated = new AtomicLong(0);

    protected long limit;

    public AbstractThreshold(long limit) {
        this.limit = limit;
    }

    protected void tryPass(long quantity) {

        gate.tryPass();

        checkCloseGate(accumulated.addAndGet(quantity));
    }

    protected void release(long quantity) {

        checkOpenGate(accumulated.addAndGet(-quantity));
    }

    protected void reset() {
        accumulated.set(0);
        checkOpenGate(0);
    }

    private void checkCloseGate(long current) {

        if(gate.isOpen() && (current >= limit)){
            logger.debug("[checkCloseGate][close gate]{}, {} / {}", gate, accumulated, limit);
            gate.close();
            //just in case, before gate.close(), all flushed
            checkOpenGate(accumulated.get());
        }
    }

    private void checkOpenGate(long current){

        if(!gate.isOpen() && (current <= (limit >> 2))){
            logger.debug("[checkOpenGate][open gate]{}, {} / {}", gate, accumulated, limit);
            gate.open();
        }
    }
}
