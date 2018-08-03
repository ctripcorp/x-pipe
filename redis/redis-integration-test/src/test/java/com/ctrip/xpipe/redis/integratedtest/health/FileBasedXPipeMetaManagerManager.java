package com.ctrip.xpipe.redis.integratedtest.health;

import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;

/**
 * @author chen.zhu
 * <p>
 * Aug 06, 2018
 */
public class FileBasedXPipeMetaManagerCollector implements XPipeMetaManagerCollector {

    private static final String KEY_XPIPE_META_FILE_PATH = "xpipe.meta.file.path";

    private String filePath = System.getProperty(KEY_XPIPE_META_FILE_PATH, "/opt/data/healthcheck/delay.xml");

    public String getFilePath() {
        return filePath;
    }

    public FileBasedXPipeMetaManagerCollector setFilePath(String filePath) {
        this.filePath = filePath;
        return this;
    }

    @Override
    public XpipeMetaManager getXPipeMetaManager() {
        return DefaultXpipeMetaManager.buildFromFile(filePath);
    }
}
