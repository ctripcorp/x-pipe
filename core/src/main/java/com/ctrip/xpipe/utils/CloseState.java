package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 19, 2018
 */
public class CloseState {

    private static final int INIT = 0;
    private static final int CLOSING = 1;
    private static final int CLOSED = 2;
    private static final Logger logger = LoggerFactory.getLogger(CloseState.class);


    private AtomicInteger state = new AtomicInteger(INIT);


    public CloseState() {
    }

    public boolean isOpen(){
        return state.get() == INIT;
    }

    public boolean isClosing(){
        return state.get() == CLOSING;
    }

    public boolean isClosed(){
        return state.get() == CLOSED;
    }

    public void setClosing() {

        while (true) {
            int previous = state.get();
            if (previous == CLOSED) {
                throw new CloseStateException("already closed, can not set closing");
            }
            boolean success = state.compareAndSet(previous, CLOSING);
            if(success){
                logger.debug("{} -> {}", previous, CLOSING);
                break;
            }
        }
    }

    public void setClosed() {
        int previous = state.getAndSet(CLOSED);
        logger.debug("{} -> {}", previous, CLOSED);

    }

    public void makeSureNotClosed() {

        if(state.get() == CLOSED){
            throw new CloseStateException("already closed");
        }

    }

    public void makeSureOpen() {
        if(state.get() != INIT){
            throw new CloseStateException("not open:" + state.get());
        }

    }

    public static class CloseStateException extends XpipeRuntimeException {

        public CloseStateException(String message) {
            super(message);
        }
    }

    @Override
    public String toString() {
        return String.valueOf(state.get());
    }
}
