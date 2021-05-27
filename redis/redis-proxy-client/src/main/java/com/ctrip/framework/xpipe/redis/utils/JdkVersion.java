package com.ctrip.framework.xpipe.redis.utils;

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

}
