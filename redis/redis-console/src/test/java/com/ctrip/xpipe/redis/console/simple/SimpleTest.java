package com.ctrip.xpipe.redis.console.simple;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         May 05, 2017
 */
public class SimpleTest extends AbstractConsoleTest{


    @Test
    public void testTime(){

        System.out.println(TimeUnit.NANOSECONDS.toMillis(1000000));

    }

    @Test
    public void testExecutors(){


        for(int i = 0;i<10;i++){

            int finalI = i;

            executors.execute(new Runnable() {
                @Override
                public void run() {
                    logger.info("[run]{}", finalI);
                    sleep(10000);
                }
            });

        }

        sleep(10000000);

    }
}
