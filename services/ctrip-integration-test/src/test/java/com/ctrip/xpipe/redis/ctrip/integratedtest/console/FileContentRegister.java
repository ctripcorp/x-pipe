package com.ctrip.xpipe.redis.ctrip.integratedtest.console;

import com.ctrip.xpipe.utils.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @author lishanglin
 * date 2021/4/21
 */
public class FileContentRegister {

    private File file;

    private String content;

    private static final Logger logger = LoggerFactory.getLogger(FileContentRegister.class);

    public FileContentRegister(File file) {
        this.file = file;
    }

    public void loadFileContent() throws Exception {
        if (file.exists()) {
            content = IOUtil.toString(new FileInputStream(file));
        }
    }

    public void restoreFileContent() {
        if (!file.exists()) {
            return;
        }

        try {
            if (null != content) {
                IOUtil.copy(content, new FileOutputStream(file));
            } else {
                file.delete();
            }
        } catch (Exception e) {
            logger.info("[storeFileContent] recover file fail {}", file.getName(), e);
        }
    }

}
