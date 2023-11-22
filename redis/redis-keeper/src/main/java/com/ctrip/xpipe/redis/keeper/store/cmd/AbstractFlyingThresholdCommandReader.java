package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.redis.core.store.CommandReader;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.utils.Gate;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lishanglin
 * date 2022/5/5
 */
public abstract class AbstractFlyingThresholdCommandReader<R> implements CommandReader<R> {

    private Gate gate;

    private AtomicLong flying = new AtomicLong(0);

    private long flyingThreshold;

    protected abstract Logger getLogger();
    protected abstract R doRead(long milliSeconds) throws IOException;

    public AbstractFlyingThresholdCommandReader(CommandStore commandStore, long flyingThreshold) {
        this.flyingThreshold = flyingThreshold;
        this.gate = new Gate(commandStore.simpleDesc());
    }

    @Override
    public R read(long miliSeconds) throws IOException {
        gate.tryPass();

        R cmdContent = doRead(miliSeconds);

        if (null != cmdContent) checkCloseGate(flying.incrementAndGet());
        return cmdContent;
    }

    @Override
    public R read() throws IOException {
        return read(-1);
    }

    private void checkCloseGate(long current) {

        debugPrint(current);

        if(gate.isOpen() && (current >= flyingThreshold)){
            getLogger().info("[increaseFlying][close gate]{}, {}", gate, current);
            gate.close();
            //just in case, before gate.close(), all flushed
            checkOpenGate(flying.get());
        }
    }

    private void debugPrint(long current) {

        if(getLogger().isDebugEnabled() && (current > 4)){
            int intCurrent = (int) current;
            if((intCurrent & (intCurrent-1)) == 0){
                getLogger().debug("[flying]{}, {}", gate, current);
            }
        }
    }

    private void checkOpenGate(long current){

        debugPrint(current);

        if(!gate.isOpen() && (current <= (flyingThreshold >> 2))){
            getLogger().info("[decreaseFlying][open gate]{}, {}", gate, flying);
            gate.open();
        }
    }

    @Override
    public void flushed(Object cmdContent){
        checkOpenGate(flying.decrementAndGet());
    }

}
