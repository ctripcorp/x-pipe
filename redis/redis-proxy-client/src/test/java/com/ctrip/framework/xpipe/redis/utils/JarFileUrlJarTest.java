package com.ctrip.framework.xpipe.redis.utils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.ctrip.framework.xpipe.redis.utils.JarFileUrlJar.TMP_PATH;

public class JarFileUrlJarTest {

    private static final String JAR_FILE_PREFIX = "jar:file:";

    private static final String REQUESTED_JAR_FILE = "BOOT-INF/lib/redis-proxy-client.jar";

    private JarFileUrlJar jarFileUrlJar;

    @Before
    public void setUp() throws IOException {
        String path = JarFileUrlJarTest.class.getClassLoader().getResource("test.jar").getFile();
        String finalPath = JAR_FILE_PREFIX + path + "!/" + REQUESTED_JAR_FILE;
        jarFileUrlJar = new JarFileUrlJar(new URL(finalPath));
    }

    @After
    public void tearDown() {
        jarFileUrlJar.close();
    }

    @Test
    public void testGetJarFilePath() throws IOException {
        String path = jarFileUrlJar.getJarFilePath();
        Assert.assertEquals(path, TMP_PATH + REQUESTED_JAR_FILE);
        Assert.assertTrue(Files.exists(Paths.get(path)));
        Assert.assertTrue(Files.deleteIfExists(Paths.get(path)));
    }

}