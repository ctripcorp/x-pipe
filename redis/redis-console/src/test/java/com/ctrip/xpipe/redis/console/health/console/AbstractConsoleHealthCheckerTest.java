package com.ctrip.xpipe.redis.console.health.console;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Nov 30, 2017
 */
public class AbstractConsoleHealthCheckerTest {

    private Future future = null;

    private int counter = 0;

    private int executeTime = 0;

    @Test
    public void testSchedule() {
        ScheduledExecutorService schedule = Executors.newScheduledThreadPool(1);

        future = schedule.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("Run for time - " + (executeTime + 1));
                if(stop()) {
                    future.cancel(true);
                    return;
                }
                executeTime ++;
            }
        }, 0, 1, TimeUnit.SECONDS);

        while(!future.isDone() || !future.isCancelled()) {
            try {
                System.out.println("isCancelled: " + future.isCancelled());
                System.out.println("isDone: " + future.isDone());
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Assert.assertTrue(future.isCancelled() || future.isDone());
        Assert.assertEquals(2, executeTime);
    }

    private boolean stop() {
        counter ++;
        System.out.println("counter: " + counter);
        if(counter < 3) {
            System.out.println("stop");
            return false;
        }
        return true;
    }
}