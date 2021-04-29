package com.ctrip.framework.xpipe.redis.utils;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

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

    @Test
    public void test() {
        System.out.println("");
    }

}