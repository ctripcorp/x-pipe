package com.ctrip.framework.xpipe.redis.utils;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class Tools {

    private static URLClassLoader jdkToolClassLoader;

    private static final Object loaderLocker = new Object();

    public static Class<?> loadJDKToolClass(String name) throws MalformedURLException, ClassNotFoundException {

        if (jdkToolClassLoader == null) {
            synchronized (loaderLocker) {
                if (jdkToolClassLoader == null) {
                    String javaPath = System.getenv("JAVA_HOME");
                    String path = javaPath + "/lib/tools.jar";
                    URL jarURl = new File(path).toURI().toURL();
                    if(JdkVersion.isJavaVersionLessThan(9)) {    // jdk < 9
                        jdkToolClassLoader = new URLClassLoader(new URL[]{jarURl}, null);   // parent=null: force to load only from input jar path
                    } else {    // jdk >= 9
                        // Make sure system property "jdk.attach.allowAttachSelf=true" is set in VM-Option
                        jdkToolClassLoader = new URLClassLoader(new URL[]{jarURl});
                    }
                }
            }
        }
        return jdkToolClassLoader.loadClass(name);
    }

    private static String pid;

    private static final Object locker = new Object();

    public static String currentPID() {
        if (pid == null) {
            synchronized (locker) {
                if (pid == null) {
                    final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
                    final int index = jvmName.indexOf('@');
                    pid = jvmName.substring(0, index);
                }
            }
        }
        return pid;
    }
}
