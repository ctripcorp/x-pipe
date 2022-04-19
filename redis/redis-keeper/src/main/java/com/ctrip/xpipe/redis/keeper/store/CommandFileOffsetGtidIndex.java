package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;

import java.io.File;
import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/3/2
 */
public class CommandFileOffsetGtidIndex implements Comparable<CommandFileOffsetGtidIndex> {

    private GtidSet excludedGtidSet;

    private File file;

    private long fileOffset;

    private static final String delimiting = "\\s*@\\s*";

    public CommandFileOffsetGtidIndex(GtidSet excludedGtidSet, File file, long fileOffset) {
        this.excludedGtidSet = excludedGtidSet;
        this.file = file;
        this.fileOffset = fileOffset;
    }

    public GtidSet getExcludedGtidSet() {
        return excludedGtidSet;
    }

    public File getFile() {
        return file;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public static CommandFileOffsetGtidIndex createFromRawString(String rawString, File file) {
        String[] idxData = rawString.split(delimiting);
        if (idxData.length < 2) return null;
        try {
            return new CommandFileOffsetGtidIndex(new GtidSet(idxData[0]), file, Long.parseLong(idxData[1]));
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    public String buildIdxStr() {
        return excludedGtidSet.toString() + "@" + fileOffset;
    }

    @Override
    public int compareTo(CommandFileOffsetGtidIndex other) {
        if (excludedGtidSet.equals(other.getExcludedGtidSet())) return 0;
        else if (excludedGtidSet.isContainedWithin(other.getExcludedGtidSet())) return -1;
        else return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandFileOffsetGtidIndex that = (CommandFileOffsetGtidIndex) o;
        return fileOffset == that.fileOffset &&
                Objects.equals(excludedGtidSet, that.excludedGtidSet) &&
                Objects.equals(file, that.file);
    }

    @Override
    public int hashCode() {
        return Objects.hash(excludedGtidSet, file, fileOffset);
    }

    @Override
    public String toString() {
        return String.format("CmdIndex[%s->%s@%d]", excludedGtidSet.toString(), file.getName(), fileOffset);
    }
}
