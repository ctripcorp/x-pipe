package com.ctrip.xpipe.redis.proxy.compress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class AbstractRunner implements Runner {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private BaseCompressBenmark benmark;

    public AbstractRunner(BaseCompressBenmark benmark) {
        this.benmark = benmark;
    }

    @Override
    public void run(RunNotifier notifier) {

        FileOutputStream target = null;
        OutputStream os = null;
        try {
            target = new FileOutputStream(new File(benmark.getCompressed()));
            os = getCompressOutputStream(target);
            Path path = new File(benmark.getRaw()).toPath();
            notifier.fireCompressStarted();
            Files.copy(path, os);
        } catch (Exception e) {
            logger.error("[compress]", e);
        } finally {
            if(target != null) {
                try {
                    target.close();
                } catch (IOException e) {
                    logger.error("", e);
                }
            }
        }
        notifier.fireCompressFinished();

        FileInputStream decompressSrc = null;
        Path decompressTarget = null;
        try {
            decompressSrc = new FileInputStream(new File(benmark.getCompressed()));
            decompressTarget = new File(benmark.getDecompressed()).toPath();
            notifier.fireDecompressStarted();
            Files.copy(getDecompressInputStream(decompressSrc), decompressTarget);
        } catch (Exception e) {
            logger.error("[decompress]", e);
        } finally {
            if(decompressSrc != null) {
                try {
                    decompressSrc.close();
                } catch (IOException e) {
                    logger.error("", e);
                }
            }
        }
        notifier.fireDecompressFinished();
    }

    protected abstract OutputStream getCompressOutputStream(OutputStream outputStream);

    protected abstract InputStream getDecompressInputStream(InputStream inputStream);
}
