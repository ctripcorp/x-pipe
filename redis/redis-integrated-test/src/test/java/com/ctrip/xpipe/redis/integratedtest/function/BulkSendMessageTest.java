package com.ctrip.xpipe.redis.integratedtest.function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class BulkSendMessageTest {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Jedis masterJedis;
    private Jedis slaveJedis;
    private String someKeyPrefix;
    private boolean shouldCleanUp;

    @Before
    public void setUp() throws Exception {
        Properties properties = loadFromLocalFile("/opt/data/100004376/function_test.properties");
        if (properties == null) {
            properties = loadFromClassPath("function_test.properties");
        }

        String masterIp = properties.getProperty("master.ip", "127.0.0.1");
        String slaveIp = properties.getProperty("slave.ip", "127.0.0.1");
        int masterPort = Integer.parseInt(properties.getProperty("master.port", "6379"));
        int slavePort = Integer.parseInt(properties.getProperty("slave.port", "6379"));

        masterJedis = createJedis(masterIp, masterPort);
        slaveJedis = createJedis(slaveIp, slavePort);

        someKeyPrefix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        shouldCleanUp = true;
    }

    @After
    public void tearDown() throws Exception {
        if (!shouldCleanUp) {
            return;
        }

        Set<String> keys = masterJedis.keys(String.format("%s*", someKeyPrefix));
        String[] arrayKeys = new String[keys.size()];
        keys.toArray(arrayKeys);
        masterJedis.del(arrayKeys);
    }

    @Test
    public void testBulkSendMessage() throws Exception {
        int count = 100;

        //setup
        for (int i = 0; i < count; i++) {
            String key = String.format("%s-bulk-%d", someKeyPrefix, i);

            masterJedis.set(key, String.valueOf(i));
        }

        logger.info("Finished bulk send message");

        TimeUnit.SECONDS.sleep(5);

        //verify
        for (int i = 0; i < count; i++) {
            String key = String.format("%s-bulk-%d", someKeyPrefix, i);

            assertEquals(String.valueOf(i), slaveJedis.get(key));
        }
    }

    private Jedis createJedis(String ip, int port) {
        Jedis jedis = new Jedis(ip, port);
        logger.info("[CreatedJedis]{}", jedis);
        return jedis;
    }

    private Properties loadFromClassPath(String fileName) {
        InputStream in = ClassLoader.getSystemResourceAsStream(fileName);

        return loadFromInputStream(in);
    }

    private Properties loadFromLocalFile(String filePath) {
        Properties properties = null;
        File file = new File(filePath);

        if (file.isFile() && file.canRead()) {
            try {
                properties = loadFromInputStream(new FileInputStream(file));
            } catch (FileNotFoundException e) {
                //ignore
            }
        }

        return properties;
    }

    private Properties loadFromInputStream(InputStream in) {
        Properties properties = null;

        try {
            properties = new Properties();
            properties.load(in);
        } catch (IOException ex) {
            //ignore
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore
            }
        }

        return properties;
    }
}
