package com.ctrip.xpipe.redis.core.store;

import java.io.File;
import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/4/17
 */
public class CommandFile {

    private File file;

    private long startOffset;

    public CommandFile(File file, long startOffset) {
        this.file = file;
        this.startOffset = startOffset;
    }

    public File getFile() {
        return file;
    }

    public long getStartOffset() {
        return startOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandFile that = (CommandFile) o;
        return startOffset == that.startOffset &&
                Objects.equals(file, that.file);
    }

    @Override
    public int hashCode() {
        return Objects.hash(file, startOffset);
    }
}
