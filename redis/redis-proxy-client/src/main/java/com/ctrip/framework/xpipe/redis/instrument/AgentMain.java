package com.ctrip.framework.xpipe.redis.instrument;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.jar.JarFile;

import static com.ctrip.framework.xpipe.redis.utils.Constants.CONNECT_CLASS;

public class AgentMain {

    public static void agentmain(String agentArgs, Instrumentation instrumentation) throws IOException {
        ClassLoader parent = ClassLoader.getSystemClassLoader().getParent();
        Class<?> connectionClass = null;
        if (parent != null) {
            try {
                connectionClass = parent.loadClass(CONNECT_CLASS.replace("/", "."));
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
