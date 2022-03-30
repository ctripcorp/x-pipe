package com.ctrip.framework.xpipe.redis.instrument;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.util.jar.JarFile;

import static com.ctrip.framework.xpipe.redis.utils.Constants.*;

public class AgentMain {

    public static void agentmain(String agentArgs, Instrumentation instrumentation) throws IOException, UnmodifiableClassException {
        if (!isProxyJarReady() && agentArgs != null) {
            File proxyJarFile = new File(agentArgs);
            instrumentation.appendToBootstrapClassLoaderSearch(new JarFile(proxyJarFile));
        }

        instrumentation.addTransformer(new ProxyAgent(), true);
        Class[] classes = instrumentation.getAllLoadedClasses();
        for (Class clazz : classes) {
            String className = clazz.getName();
            if (SOCKET.equals(className) || NIO_SOCKET.equals(className) || ABSTRACT_NIO_SOCKET.equals(className)) {
                instrumentation.retransformClasses(clazz);
            }
        }
    }

    // CONNECT_CLASS must can be found by bootstrap classloader
    // otherwise class loaded by bootstrap classloader can't access CONNECT_CLASS
    public static boolean isProxyJarReady() {
        ClassLoader parent = ClassLoader.getSystemClassLoader().getParent();
        Class<?> connectionClass = null;
        if (parent != null) {
            try {
                connectionClass = parent.loadClass(CONNECT_CLASS.replace("/", "."));
            } catch (Throwable e) {
                // ignore
            }
        }

        return null != connectionClass;
    }

}
