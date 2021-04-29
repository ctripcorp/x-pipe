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

/**
 * @Author limingdong
 * @create 2021/4/29
 */
public class JarFileUrlJarTest {

    private static final String JAR_FILE = "jar:file:/Users/limingdong/ctrip/drc/package/drc-applier-package/target/drc-applier-package-0.0.1.jar!/BOOT-INF/lib/redis-proxy-client-1.2.2.jar";

    private JarFileUrlJar jarFileUrlJar;

    @Before
    public void setUp() throws IOException {
        jarFileUrlJar = new JarFileUrlJar(new URL(JAR_FILE));
    }

    @After
    public void tearDown() {
        jarFileUrlJar.close();
    }

    @Test
    public void testGetJarFilePath() throws IOException {
        String path = jarFileUrlJar.getJarFilePath();
        Assert.assertEquals(path, TMP_PATH + "BOOT-INF/lib/redis-proxy-client-1.2.2.jar");
        Assert.assertTrue(Files.exists(Paths.get(path)));
        Assert.assertTrue(Files.deleteIfExists(Paths.get(path)));
    }

}