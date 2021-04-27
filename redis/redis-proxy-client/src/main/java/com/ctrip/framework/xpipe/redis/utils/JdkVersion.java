package com.ctrip.framework.xpipe.redis.utils;

/**
 *
 'java.version' system property to show JDK version.
    Java 8 or lower: 1.6.0_23, 1.7.0, 1.7.0_80, 1.8.0_211
    Java 9 or higher: 9.0.1, 11.0.4, 12, 12.0.1

 */
public class JdkVersion {
    public static final String JAVA_VERSION = "java.version";

    public static int getVersion() {
        String version = System.getProperty(JAVA_VERSION);
        if(version.startsWith("1.")) {
            version = version.substring(2, 3);
        } else {
            int dot = version.indexOf(".");
            if(dot != -1) {
                version = version.substring(0, dot);
            }
        }
        return Integer.parseInt(version);
    }

    public static boolean isJavaVersionLessThan(int upVersion) {
        return getVersion() < upVersion;
    }

    public static boolean isJavaVersionAtLeast(int requiredVersion) {
        return getVersion() >= requiredVersion;
    }


}
