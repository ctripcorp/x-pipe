package com.ctrip.xpipe.utils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class TestAbstractScriptExecutor {

    private static Logger logger = LoggerFactory.getLogger(TestAbstractScriptExecutor.class);

    @Test
    public void testAbstractScriptExecutor() throws ExecutionException, InterruptedException {
        List<String> lines = new LsScriptExecutor().execute().get();
        for(String line : lines) {
            logger.info("{}", line);
        }
    }

    @Test
    public void testAbstractScriptExecutorNetstat() throws ExecutionException, InterruptedException {
        List<String> lines = new NetstatScriptExecutor().execute().get();
        for(String line : lines) {
            logger.info("{}", line);
        }
    }

    private class LsScriptExecutor extends AbstractScriptExecutor<List<String>> {

        @Override
        public String getScript() {
            return "ls -lh";
        }

        @Override
        public List<String> format(List<String> result) {
            return result;
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }
    }

    private class NetstatScriptExecutor extends AbstractScriptExecutor<List<String>> {

        @Override
        public String getScript() {
            return "netstat -itn";
        }

        @Override
        public List<String> format(List<String> result) {
            return result;
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }
    }

}