package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 04, 2018
 */
public class LifecycleObservableAbstractTest extends AbstractTest {

    @Test
    public void testEfficiency() {

        int size = 10;

        List<Object> objects = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            objects.add(new Object());
        }

        int count = 1 << 20;

        Object[] objects1;
        long begin = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            objects1 = objects.toArray();
        }
        long end = System.currentTimeMillis();

        logger.info("{} ns", (end - begin) * 1000000/count);



    }

    @Test
    public void testConcurrent() {

        int count = 100;
        AtomicBoolean nullObserver = new AtomicBoolean(false);
        TestLifecycle testLifecycle = new TestLifecycle(nullObserver);

        List<Observer> observers = new LinkedList<>();

        for (int i = 0; i < count; i++) {

            Observer observer = new Observer() {
                @Override
                public void update(Object args, Observable observable) {
                }
            };
            observers.add(observer);
            testLifecycle.addObserver(observer);
        }

        executors.execute(new Runnable() {
            @Override
            public void run() {

                observers.forEach(observer -> testLifecycle.removeObserver(observer));
            }
        });

        for (int i = 0; i < count * 10; i++) {
            testLifecycle.notifyObservers(new Object());
        }

        Assert.assertFalse(nullObserver.get());

    }


    private static class TestLifecycle extends AbstractLifecycleObservable {

        private AtomicBoolean nullObserver;

        public TestLifecycle(AtomicBoolean nullObserver) {
            this.nullObserver = nullObserver;
            nullObserver.set(false);
        }

        @Override
        protected void beginNotifyObserver(Object observer) {
            if (observer == null) {
                nullObserver.set(true);
            }
        }

        @Override
        public void notifyObservers(Object arg) {
            super.notifyObservers(arg);
        }
    }

}
