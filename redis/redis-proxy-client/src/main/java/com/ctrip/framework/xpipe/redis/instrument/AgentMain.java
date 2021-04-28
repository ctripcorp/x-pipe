package com.ctrip.framework.xpipe.redis.instrument;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.jar.JarFile;

/**
 * @Author limingdong
 * @create 2021/4/26
 */
public class AgentMain {

    public static void agentmain(String agentArgs, Instrumentation instrumentation) throws IOException {
        ClassLoader parent = ClassLoader.getSystemClassLoader().getParent();
        Class<?> connectionClass = null;
        if (parent != null) {
            try {
                connectionClass = parent.loadClass("com.ctrip.framework.xpipe.redis.utils.ConnectionUtil");
            } catch (Throwable e) {
                // ignore
            }
        }
        if (connectionClass == null) {
            File proxyJarFile = new File(agentArgs);
            instrumentation.appendToBootstrapClassLoaderSearch(new JarFile(proxyJarFile));
        }
        instrumentation.addTransformer(new ProxyAgent(), true);
    }

}
