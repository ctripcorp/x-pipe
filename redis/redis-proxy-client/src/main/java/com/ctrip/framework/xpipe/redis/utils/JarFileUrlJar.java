package com.ctrip.framework.xpipe.redis.utils;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.jar.JarFile;

public class JarFileUrlJar {

    private JarFile jarFile;

    public JarFileUrlJar(URL url) throws IOException {
        try {
            // jar:file:...
            JarURLConnection jarConn = (JarURLConnection) url.openConnection();
            jarConn.setUseCaches(false);
            jarFile = jarConn.getJarFile();
        } catch (Exception e) {
            throw new IOException(jarFile.getName() + ":" + jarFile.size(), e);
        }
    }

    public void close() {
        if (jarFile != null) {
            try {
                jarFile.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    public JarFile getJarFile() {
        return jarFile;
    }
}
