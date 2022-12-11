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
public abstract class AbstractThreshold implements Threshold {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Gate gate = new Gate(getClass().getSimpleName());

    protected AtomicLong accumulated = new AtomicLong(0);

    protected long limit;

    public AbstractThreshold(long limit) {
        this.limit = limit;
    }

    @Override
    public void tryPass(long quantity) {

        gate.tryPass();

        checkCloseGate(accumulated.addAndGet(quantity));
    }

    @Override
    public void release(long quantity) {

        checkOpenGate(accumulated.addAndGet(-quantity));
    }

    private void checkCloseGate(long current) {

        if(gate.isOpen() && (current >= limit)){
            logger.info("[checkCloseGate][close gate]{}, {} / {}", gate, accumulated, limit);
            gate.close();
            //just in case, before gate.close(), all flushed
            checkOpenGate(accumulated.get());
        }
    }

    private void checkOpenGate(long current){

        if(!gate.isOpen() && (current <= (limit >> 2))){
            logger.info("[checkOpenGate][open gate]{}, {} / {}", gate, accumulated, limit);
            gate.open();
        }
    }
}
