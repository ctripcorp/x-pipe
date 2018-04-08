package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.retry.RetryDelay;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chen.zhu
 * <p>
 * Apr 02, 2018
 */
public class AdvancedDcMetaServiceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DcMetaService dcMetaService;

    @Autowired
    private AdvancedDcMetaService service;

    @Test
    public void testRetry3TimesUntilSuccess() throws Exception {
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
        service.setScheduled(scheduled).setFactory(new DefaultRetryCommandFactory(3, new RetryDelay(10), scheduled));
        Command<String> command = service.retry3TimesUntilSuccess(new AbstractCommand<String>() {

            private AtomicInteger counter = new AtomicInteger(0);

            @Override
            protected void doExecute() throws Exception {
                int currentCount = counter.getAndIncrement();
                logger.info(String.format("Run %d time", currentCount));
                if(currentCount > 1) {
                    future().setSuccess("success");
                } else {
                    throw new XpipeRuntimeException("test exception");
                }
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "test-retry";
            }
        });

        AtomicBoolean complete = new AtomicBoolean(false);
        command.future().addListener(commandFuture -> {
            Assert.assertEquals("success", commandFuture.getNow());
            complete.getAndSet(true);
        });

        command.execute();

        waitConditionUntilTimeOut(() -> complete.get());
    }

    @Test
    public void testGetDcMeta() {
        long start = System.currentTimeMillis();
        dcMetaService.getDcMeta(dcNames[0]);
        long end = System.currentTimeMillis();
        logger.info("[duration] {}", end - start);
    }

    @Test
    public void testGetDcMeta2() {
        long start = System.currentTimeMillis();
        for(int i = 0; i < 100; i++) {
            dcMetaService.getDcMeta(dcNames[(1&i)]);
        }
        long end = System.currentTimeMillis();
        logger.info("[duration] {}", (end - start)/100);
    }


    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }
}