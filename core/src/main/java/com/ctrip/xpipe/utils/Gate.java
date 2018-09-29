package com.ctrip.xpipe.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 29, 2018
 */
public class Gate {

    private AtomicBoolean isOpen = new AtomicBoolean(true);

    private Queue<Thread> passengers = new ConcurrentLinkedDeque<>();

    private String name;

    private static final Logger logger = LoggerFactory.getLogger(Gate.class);

    public Gate(String name) {
        this.name = name;

    }

    public boolean isOpen() {
        return isOpen.get();
    }

    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            logger.info("[close][{}]", name);
        }
    }

    public void tryPass() {

        if (isOpen.get()) {
            return;
        }

        if (isOpen.get() == false) {

            Thread currentThread = Thread.currentThread();
            passengers.offer(currentThread);


            if (isOpen.get() == true) {
                passengers.remove(currentThread);
                return;
            }

            logger.info("[pass][{}]park", name);
            LockSupport.park();
            logger.info("[pass][{}][park finished]", name);

        }
    }

    public void open() {

        if (isOpen.compareAndSet(false, true)) {
            logger.info("[open][{}]", name);
            makeAllPass();
        }
    }

    private void makeAllPass() {

        Thread passenger;
        while ((passenger = passengers.poll()) != null) {
            logger.info("[makeAllPass][{}], {}", name, passenger);
            LockSupport.unpark(passenger);
        }
    }

    @Override
    public String toString() {
        return String.format("Gate:%s", name);
    }
}
