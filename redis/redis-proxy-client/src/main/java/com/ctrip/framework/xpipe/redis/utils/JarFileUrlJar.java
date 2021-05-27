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
import java.util.stream.Stream;

public class JarFileUrlJar {

    public static final String TMP_PATH = "/tmp/redis/proxy/";

    private JarFile jarFile;

    private JarEntry entry;

    private String fileName;

    public JarFileUrlJar(URL url) throws IOException {
        try {
            // jar:file:...
            JarURLConnection jarConn = (JarURLConnection) url.openConnection();
            jarConn.setUseCaches(false);
            jarFile = jarConn.getJarFile();
            entry = jarConn.getJarEntry();
            fileName = TMP_PATH + entry.getName();
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

    private InputStream getEntryInputStream() throws IOException {
        if (entry == null) {
            return null;
        } else {
            return jarFile.getInputStream(entry);
        }
    }

    public String getJarFilePath() throws IOException {
        InputStream inputStream = getEntryInputStream();
        Files.copy(inputStream, getDistFile(fileName));
        return fileName;
    }

    private Path getDistFile(String path) throws IOException {

        Path dist = Paths.get(path);
        Path parent = dist.getParent();
        if (parent != null) {
            if (Files.exists(parent)) {
                delete(parent);
            }
            Files.createDirectories(parent);
        }
        return dist;
    }

    private void delete(Path root) throws IOException {
        Stream<Path> children =  Files.list(root);
        children.forEach(p -> {
            try {
                Files.deleteIfExists(p);
            } catch (IOException e) {
            }
        });
    }
}
