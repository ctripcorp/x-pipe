package com.ctrip.xpipe.redis.integratedtest.console;


import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


/**
 * @author chen.zhu
 * <p>
 * Nov 21, 2017
 */
public class TestShutDown {

    private static final Logger logger = LoggerFactory.getLogger(TestShutDown.class);

    @Test
    public void testShutDownTime() throws Exception {

        List<String> result = runScript("src/test/resources/scripts/getConsoleShutDownTime.sh");

        int elapsedTime = Integer.parseInt(result.get(result.size() - 1));

        logger.info("[testShutDownTime] execute result:");

        result.forEach(line -> logger.info("[testShutDownTime] {}", line));

        Assert.assertTrue(elapsedTime < 30);
    }

    private List<String> runScript(final String filename) throws Exception {
        final Runtime runtime = Runtime.getRuntime();
        final Process process = runtime.exec(filename);
        process.waitFor();
        final BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream()));

        List<String> result = new ArrayList<>();
        String line = null;
        while((line = reader.readLine()) != null) {
            result.add(line);
        }
        return result;
    }
}
