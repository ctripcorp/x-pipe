package com.ctrip.framework.xpipe.redis.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class JarFileUrlJar {

    private static final String TMP_PATH = "/tmp/redis/proxy/";

    private final JarFile jarFile;

    private JarEntry entry;

    private String fileName;

    public JarFileUrlJar(URL url) throws IOException {
        // jar:file:...
        JarURLConnection jarConn = (JarURLConnection) url.openConnection();
        jarConn.setUseCaches(false);
        jarFile = jarConn.getJarFile();
        entry = jarConn.getJarEntry();
        fileName = TMP_PATH + entry.getName();
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

    private InputStream getEntryInputStream() throws IOException {
        if (entry == null) {
            return null;
        } else {
            return jarFile.getInputStream(entry);
        }
    }

    public String getJarFilePath() throws IOException {
        InputStream inputStream = getEntryInputStream();
        Files.deleteIfExists(Paths.get(fileName));
        Files.copy(inputStream, getDistFile(fileName));
        return fileName;
    }

    private Path getDistFile(String path) throws IOException {

        Path dist = Paths.get(path);
        Path parent = dist.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        return dist;
    }
}
